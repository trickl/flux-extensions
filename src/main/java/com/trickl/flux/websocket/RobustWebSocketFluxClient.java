package com.trickl.flux.websocket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trickl.exceptions.ConnectionTimeoutException;
import com.trickl.exceptions.MissingHeartbeatException;
import com.trickl.exceptions.NoDataException;
import com.trickl.exceptions.RemoteStreamException;
import com.trickl.flux.mappers.DecodingTransformer;
import com.trickl.flux.mappers.EncodingTransformer;
import com.trickl.flux.mappers.ExpectedResponseTimeoutFactory;
import com.trickl.flux.mappers.FluxSinkAdapter;
import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.mappers.ThrowingFunction;
import com.trickl.flux.publishers.CacheableResource;
import com.trickl.flux.publishers.ConcatProcessor;
//import com.trickl.flux.publishers.SwitchableProcessor;
import com.trickl.flux.retry.ExponentialBackoffRetry;
import com.trickl.flux.routing.TopicRouter;
import com.trickl.flux.routing.TopicSubscription;
import java.io.IOException;
import java.net.URI;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@Log
@Builder
public class RobustWebSocketFluxClient<S, T> {
  private final WebSocketClient webSocketClient;

  @Getter private final Supplier<URI> transportUriProvider;

  @Getter private final BiFunction<FluxSink<S>, Publisher<S>, WebSocketHandler> handlerFactory;

  @Builder.Default private Supplier<HttpHeaders> webSocketHeadersProvider = HttpHeaders::new;

  @Builder.Default private Duration disconnectionReceiptTimeout = Duration.ofSeconds(5);

  @Builder.Default private Duration initialRetryDelay = Duration.ofSeconds(1);

  @Builder.Default private Duration retryConsiderationPeriod = Duration.ofSeconds(255);

  @Builder.Default private int maxRetries = 8;

  @Builder.Default private Mono<Void> doBeforeSessionOpen = Mono.empty();

  @Builder.Default private Mono<Void> doAfterSessionClose = Mono.empty();

  @Builder.Default
  private Function<FluxSink<T>, Duration> doConnect = sink -> Duration.ZERO;

  @Builder.Default private Predicate<T> isConnectedFrame = frame -> true;

  @Builder.Default private Predicate<T> isReceiptFrame = frame -> true;

  @Builder.Default private BiPredicate<T, String> isDataFrameForDestination =
      (frame, destination) -> true;

  @Builder.Default
  private Function<T, Duration> getHeartbeatSendFrequencyCallback =
      connectedFrame -> Duration.ofSeconds(10);

  @Builder.Default
  private Function<T, Duration> getHeartbeatReceiveFrequencyCallback =
      connectedFrame -> Duration.ofSeconds(10);

  @Builder.Default
  private Supplier<Optional<T>> buildDisconnectFrame = () -> Optional.empty();

  @Builder.Default
  private Function<Set<TopicSubscription>, List<T>> buildSubscribeFrames = 
      (Set<TopicSubscription> topics) -> Collections.emptyList();

  @Builder.Default
  private Function<Set<TopicSubscription>, List<T>> buildUnsubscribeFrames = 
      (Set<TopicSubscription> topics) -> Collections.emptyList();

  @Builder.Default
  private Function<Throwable, Optional<T>> buildErrorFrame = 
      (Throwable error) -> Optional.empty();

  @Builder.Default
      private Function<Long, Optional<T>> buildHeartbeatFrame = 
          (Long count) -> Optional.empty();

  @Builder.Default
  private Function<T, Optional<Throwable>> decodeErrorFrame = 
      (T frame) -> Optional.empty();

  @Builder.Default
  private ThrowingFunction<T, S, IOException> encoder = frame -> null;

  @Builder.Default
  private ThrowingFunction<S, List<T>, IOException> decoder = bytes -> Collections.emptyList();

  @Builder.Default
  private Duration subscriptionThrottleDuration = Duration.ofSeconds(1);

  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_RED = "\u001B[31m";

  private final CacheableResource<SharedStreamContext<T>> sharedStreamContext
      = new CacheableResource<SharedStreamContext<T>>(
          context -> getSharedStreamContext().cache(1).next(),
          context -> {
            return !context.getConnectedStreamContext()
            .getResponseContext().getIsTerminated().get(); 
          });

  protected TopicContext<T> createTopicContext() {
    log.info("Creating topic context");
    EmitterProcessor<Long> connectedSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> connectedSignalSink = connectedSignalEmitter.sink();

    EmitterProcessor<Long> disconnectedSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> disconnectedSignalSink = disconnectedSignalEmitter.sink();

    Publisher<Long> connectedSignal = connectedSignalEmitter
        .log(ANSI_RED + "connectedSignal" + ANSI_RESET).share();
    Publisher<Long> disconnectedSignal = disconnectedSignalEmitter
        .log(ANSI_RED + "disconnectedSignal" + ANSI_RESET).share();

    TopicRouter<T> topicRouter = TopicRouter.<T>builder()
        .startConnected(false)
        .connectedSignal(connectedSignal) 
        .disconnectedSignal(disconnectedSignal)
        .subscriptionThrottleDuration(subscriptionThrottleDuration)
        .topicFilter(destination -> frame -> isDataFrameForDestination.test(frame, destination))    
        .build();

    return new TopicContext<T>(
      connectedSignalSink,
      connectedSignal,
      disconnectedSignalSink,   
      disconnectedSignal,
      topicRouter);
  }

  protected ResponseContext<T> createResponseContext(TopicContext<T> topicContext) {
    log.info("Creating response context");
    EmitterProcessor<Long> beforeOpenSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> beforeOpenSignalSink = beforeOpenSignalEmitter.sink();

    EmitterProcessor<T> forceReconnectSignalEmitter = EmitterProcessor.create();
    FluxSink<T> forceReconnectSignalSink = forceReconnectSignalEmitter.sink();

    //SwitchableProcessor<Duration> connectionExpectationProcessor =
    //    SwitchableProcessor.create(beforeOpenSignalEmitter, "beforeOpen");
    ConcatProcessor<Duration> connectionExpectationProcessor =
        ConcatProcessor.create();

    //SwitchableProcessor<T> streamRequestProcessor =
    //    SwitchableProcessor.create(beforeOpenSignalEmitter, "streamRequest");
    ConcatProcessor<T> streamRequestProcessor =
        ConcatProcessor.create();

    AtomicBoolean isTerminated = new AtomicBoolean(false);
    
    Publisher<Set<TopicSubscription>> subscriptions = Flux.from(
        topicContext.getTopicRouter().getSubscriptions()).log("subscriptions");
    Publisher<Set<TopicSubscription>> subscribeActionFlux = Flux.from(subscriptions)
        .doOnNext(topics -> {
          List<T> subscriptionFrames = buildSubscribeFrames.apply(topics);
          subscriptionFrames.stream().forEach(subscriptionFrame -> {
            streamRequestProcessor.sink()
                .next(subscriptionFrame);
          });          
        }).ignoreElements().log("subscriptionsActions");

    Publisher<Set<TopicSubscription>> cancellations = 
        topicContext.getTopicRouter().getCancellations();
    Publisher<Set<TopicSubscription>> unsubscribeActionFlux = Flux.from(cancellations)
        .doOnNext(topics -> {
          List<T> unsubscribeFrames = buildUnsubscribeFrames.apply(topics);
          unsubscribeFrames.stream().forEach(unsubscribeFrame -> {
            streamRequestProcessor.sink()
                .next(unsubscribeFrame);
          });          
        }).ignoreElements();

    return new ResponseContext<>(
        topicContext,
        beforeOpenSignalSink,
        forceReconnectSignalSink,
        forceReconnectSignalEmitter,
        connectionExpectationProcessor,
        streamRequestProcessor,       
        isTerminated,
        Flux.merge(subscribeActionFlux, unsubscribeActionFlux));
  }

  protected ConnectedStreamContext<T> createConnectedStreamContext(
      ResponseContext<T> responseContext, Flux<T> source, T connectedFrame) {
    log.info("Creating connected stream context");

    //SwitchableProcessor<Duration> heartbeatSendFrequencyProcessor =
    //    SwitchableProcessor.create(
    //      responseContext.getTopicContext().getConnectedSignal(), 1, "heartbeatSend");

    ConcatProcessor<Duration> heartbeatSendFrequencyProcessor =
          ConcatProcessor.create(1);

    //SwitchableProcessor<Duration> heartbeatExpectationProcessor =
    //    SwitchableProcessor.create(
    //      responseContext.getTopicContext().getConnectedSignal(), 1, "heartbeatExpectation");

    ConcatProcessor<Duration> heartbeatExpectationProcessor =
        ConcatProcessor.create(1);

    Duration heartbeatSendFrequency = getHeartbeatSendFrequencyCallback.apply(connectedFrame);
    Duration heartbeatReceiveFrequency = getHeartbeatReceiveFrequencyCallback.apply(connectedFrame);

    ExpectedResponseTimeoutFactory<T> heartbeatExpectationFactory =
        ExpectedResponseTimeoutFactory.<T>builder()
            .isRecurring(true)
            .isResponse(value -> true)
            .timeoutExceptionMapper(
                (error, frequency) -> {
                  Instant now = Instant.now();
                  DateTimeFormatter formatter =
                      DateTimeFormatter.ofLocalizedDateTime(FormatStyle.MEDIUM)
                                      .withLocale(Locale.UK)
                                      .withZone(ZoneId.systemDefault());
                  String errorMessage = MessageFormat.format("{0} - no heartbeat received for {1}",
                      formatter.format(now), frequency);
                    return new MissingHeartbeatException(errorMessage, error);
                })
            .build();

    Publisher<T> heartbeatExpectation =
        heartbeatExpectationFactory.apply(heartbeatExpectationProcessor, source);            

    Flux<T> heartbeats =
        Flux.from(heartbeatSendFrequencyProcessor)
        .log("heartbeatSwitchableProcessor")
        .<T>switchMap(
            frequency ->
                sendHeartbeats(frequency, responseContext.getStreamRequestProcessor().sink()))
        .log("heartbeats");

    Flux<T> heartbeatExpectations = Flux.from(heartbeatExpectation)
        .log("heartbeatExpectation").mergeWith(heartbeats);

    Flux<T> sourceWithExpectations = source
        .mergeWith(heartbeatExpectations)
        //.doOnError(error -> error instanceof MissingHeartbeatException, error -> {
        // Reconnect on missing heartbeat
        //  log.info("Source with Expectations forcing reconnect");
        //  responseContext.getForceReconnectSignalSink().error(error);
        //})
        
        .doOnError(MissingHeartbeatException.class, error -> {
          log.info("Source with Expectations forcing reconnect");          
          expectHeartbeatsEvery(
              heartbeatExpectationProcessor.sink(), Duration.ZERO);
          sendHeartbeatsEvery(
              heartbeatSendFrequencyProcessor.sink(), Duration.ZERO);
          responseContext.getForceReconnectSignalSink().error(error);
        });
    /*
    .retryWhen(
        errorFlux ->            
        errorFlux.flatMap(
          error -> {
            return Mono.delay(Duration.ofSeconds(5)).doOnNext(delay -> {
              log.info("Retrying after 5 seconds delay");
            });
          })).log("sourceWithExpectations");
          */

    return new ConnectedStreamContext<>(
        responseContext,
        heartbeatExpectationProcessor,
        heartbeatSendFrequencyProcessor,
        heartbeatSendFrequency,
        heartbeatReceiveFrequency,
        sourceWithExpectations);
  }

  protected SharedStreamContext<T> createSharedStreamContext(
      ConnectedStreamContext<T> context) {
    log.info("Creating shared stream context");

    Flux<T> sharedStream =
        context
            .getStream()
            .onErrorContinue(JsonProcessingException.class, this::warnAndDropError)
            .doOnError(
                error -> {
                  sendErrorFrame(
                      error, context.getResponseContext().getStreamRequestProcessor().sink());
                })  
            //.onErrorContinue(MissingHeartbeatException.class, (error, value) -> {
            //  log.info("Shared stream forcing reconnect");
            //  context.getResponseContext().getForceReconnectSignalSink().error(error);
            //})
            // Don't cancel
            .retryWhen(
                errorFlux ->
                  errorFlux.flatMap(error -> Mono.empty()))
            /*
            .retryWhen(
                errorFlux ->            
            errorFlux.flatMap(
              error -> {
                return Mono.delay(Duration.ofSeconds(5)).doOnNext(delay -> {
                  log.info("Retrying after 5 seconds delay");
                });
              }))
              */
              
            .share()
            .log("sharedStream", Level.INFO);

    log.info("Returning SharedStreamContext.");
    return new SharedStreamContext<>(
        context,
        sharedStream);
  }

  protected Flux<SharedStreamContext<T>> getSharedStreamContext() {
    return Flux.<SharedStreamContext<T>, TopicContext<T>>usingWhen(
        Mono.fromSupplier(this::createTopicContext).cache(),
        context -> getSharedStreamContext(createResponseContext(context)),
        context -> {
          log.info("Cleaning up response context");
          return Mono.empty();
        })
        .retryWhen(
            ExponentialBackoffRetry.builder()
                .initialRetryDelay(initialRetryDelay)
                .considerationPeriod(retryConsiderationPeriod)
                .maxRetries(maxRetries)
                .name("ConnectedStreamContext")
                .build())
        .log("ConnectedStreamContext");
  }

  protected Flux<SharedStreamContext<T>> getSharedStreamContext(
      ResponseContext<T> context) {
    log.info("Creating base stream context mono");
    Publisher<T> sendWithResponse =
        Flux.merge(Flux.from(context.getStreamRequestProcessor()))
            .log("sendWithResponse", Level.FINE);

    WebSocketFluxClient<S> webSocketFluxClient =
         WebSocketFluxClient.<S>builder()
            .webSocketClient(webSocketClient)
            .transportUriProvider(transportUriProvider)
            .handlerFactory(handlerFactory)
            .webSocketHeadersProvider(Mono.fromSupplier(webSocketHeadersProvider))        
            .doBeforeOpen(doBeforeSessionOpen.then(
                Mono.fromRunnable(() -> context.getBeforeOpenSignalSink().next(1L))))
            .doAfterOpen(
                sink ->
                connect(new FluxSinkAdapter<T, S, IOException>(sink, encoder), 
                context.getConnectionExpectationProcessor()))
            .doBeforeClose(response -> Mono.fromRunnable(() -> {
              log.info("Response context is terminated.");
              context.getIsTerminated().set(true);
              context.getTopicContext().getDisconnectedSignalSink().next(1L);
            }).then(
              Mono.delay(Duration.ofMillis(500)).then())
            )
            .doAfterClose(doAfterSessionClose)
            .build();

    DecodingTransformer<S, T> inputTransformer = new DecodingTransformer<S, T>(decoder);
    EncodingTransformer<T, S> outputTransformer = new EncodingTransformer<T, S>(encoder);

    Flux<T> base =
        Flux.defer(() -> inputTransformer.apply(
          webSocketFluxClient.get(outputTransformer.apply(sendWithResponse))))
            .flatMap(new ThrowableMapper<T, T>(this::handleErrorFrame))
            .log("sharedBase")     
            .mergeWith(
              Flux.from(context.getSubscriptionActionsFlux()).map(none -> null)
            )
            .share()
            .log("base", Level.INFO);


    Flux<T> connected = base.filter(isConnectedFrame)
        .log("connection", Level.INFO);

    ExpectedResponseTimeoutFactory<T> connectionExpectationFactory =
        ExpectedResponseTimeoutFactory.<T>builder()
            .isRecurring(false)
            .isResponse(value -> true)
            .timeoutExceptionMapper(
                (error, period) ->
                    new ConnectionTimeoutException("No connection within " + period, error))
            .build();
    Publisher<T> connectionExpectation =
        connectionExpectationFactory.apply(context.getConnectionExpectationProcessor(), connected);

    Flux<T> connectedWithExpectations =
        connected
            .mergeWith(connectionExpectation)
            .mergeWith(context.getForceReconnectSignal())
            .log("sharedConnectedWithExpectations", Level.INFO)
            .share()
            .log("connectedWithExpectations", Level.INFO);

    return connectedWithExpectations.<SharedStreamContext<T>>flatMap(
        connectedFrame ->
            Mono.fromSupplier(() -> createConnectedStreamContext(
              context, connected, connectedFrame))
                .log("ConnectedStreamContextSupplier", Level.INFO)
                .<SharedStreamContext<T>>map(
                    connectedStreamContext -> {
                      connectedStreamContext
                          .getResponseContext()
                          .getTopicContext()
                          .getConnectedSignalSink()
                          .next(1L);

                      log.info("Sending / expecting heartbeats");
                      if (!connectedStreamContext.getHeartbeatReceiveFrequency().isZero()) {
                        expectHeartbeatsEvery(
                            connectedStreamContext.getHeartbeatExpectationProcessor().sink(),
                            connectedStreamContext.getHeartbeatReceiveFrequency());
                      }

                      sendHeartbeatsEvery(
                          connectedStreamContext.getHeartbeatSendFrequencyProcessor().sink(),
                          connectedStreamContext.getHeartbeatSendFrequency());
                          
                      return createSharedStreamContext(connectedStreamContext);
                    }));
  }

  protected Mono<Void> connect(
      FluxSink<T> streamRequestSink,
      ConcatProcessor<Duration> connectionExpectationProcessor) {
    Duration connectionTimeout = doConnect.apply(streamRequestSink);
    expectConnectionWithin(connectionExpectationProcessor.sink(), connectionTimeout);
    return Mono.empty();
  }

  protected Mono<Void> disconnect(
      Publisher<T> response,
      FluxSink<T> streamRequestSink,
      FluxSink<Duration> receiptExpectationSink) {
    disconnect(streamRequestSink, receiptExpectationSink);

    return Mono.from(
            Flux.from(response)
                .filter(frame -> isReceiptFrame.test(frame))                
                .log("disconnect"))
        .then();
  }

  protected void disconnect(
      FluxSink<T> streamRequestSink, FluxSink<Duration> receiptExpectationSink) {    
    Optional<T> disconnectFrame = buildDisconnectFrame.get();
    log.info("Disconnecting...");    
    if (disconnectFrame.isPresent()) {
      receiptExpectationSink.next(disconnectionReceiptTimeout);
      streamRequestSink.next(disconnectFrame.get());
    }
  }

  protected T handleErrorFrame(T frame) throws RemoteStreamException {  
    Optional<Throwable> error = decodeErrorFrame.apply(frame);
    if (error.isPresent()) {
      throw new RemoteStreamException("Remote stream encountered error", error.get());
    }
    return frame;
  }

  protected void sendErrorFrame(Throwable error, FluxSink<T> streamRequestSink) {
    log.info("Sending error frame");
    Optional<T> errorFrame = buildErrorFrame.apply(error);
    if (errorFrame.isPresent()) {
      streamRequestSink.next(errorFrame.get());
    }
  }

  protected Publisher<T> sendHeartbeats(
      Duration frequency, FluxSink<T> streamRequestSink) {
    log.info("Sending heartbeats every " + frequency.toString());
    if (frequency.isZero()) {
      return Flux.empty();
    }

    return Flux.interval(frequency)
        .map(count -> buildHeartbeatFrame.apply(count))
        .flatMap(
            optionalHeartbeat -> {
              if (optionalHeartbeat.isPresent()) {
                streamRequestSink.next(optionalHeartbeat.get());
              }
              return Mono.<T>empty();
            })
        .log("heartbeats", Level.INFO);
  }

  protected void sendHeartbeatsEvery(
      FluxSink<Duration> heartbeatSendFrequencySink, Duration frequency) {
    log.info("Request sending heartbeats every " + frequency.toString());
    heartbeatSendFrequencySink.next(frequency);
  }

  protected void expectHeartbeatsEvery(
      FluxSink<Duration> heartbeatExpectationSink, Duration frequency) {
    log.info("Expecting heartbeats every " + frequency.toString());
    heartbeatExpectationSink.next(frequency);
  }

  protected void expectConnectionWithin(
      FluxSink<Duration> connectionExpectationSink, Duration period) {
    connectionExpectationSink.next(period);
  }

  protected void warnAndDropError(Throwable ex, Object value) {
    log.log(
        Level.WARNING,
        MessageFormat.format(
            "Message: {0}\nValue: {1}\n",
            new Object[] {ex.getMessage(), value}));
  }

  /**
   * Get a flux for a destination.
   *
   * @param destination The destination channel
   * @param minMessageFrequency Unsubscribe if no message received in this time
   * @return A flux of messages on that channel
   */
  public Flux<T> get(
      String destination, Duration minMessageFrequency) {
     
    return Flux.<T, SharedStreamContext<T>>usingWhen(
      sharedStreamContext.getResource(),
      context -> context
          .getConnectedStreamContext()
          .getResponseContext()
          .getTopicContext()
          .getTopicRouter().route(
          context.getSharedStream(), destination)
      .timeout(minMessageFrequency)
      .onErrorMap(
        error -> {
          if (error instanceof TimeoutException) {
            return new NoDataException("No data within " + minMessageFrequency, error);
          }
          return error;
        }),
      context -> Mono.empty());        
  }

  @Value
  private static class TopicContext<T> {
    protected final FluxSink<Long> connectedSignalSink;

    protected final Publisher<Long> connectedSignal;

    protected final FluxSink<Long> disconnectedSignalSink;

    protected final Publisher<Long> disconnectedSignal;

    protected final TopicRouter<T> topicRouter;
  }

  @Value
  private static class ResponseContext<T> {
    protected final TopicContext<T> topicContext;

    protected final FluxSink<Long> beforeOpenSignalSink;

    protected final FluxSink<T> forceReconnectSignalSink;

    protected final Publisher<T> forceReconnectSignal;

    protected final ConcatProcessor<Duration> connectionExpectationProcessor;

    protected final ConcatProcessor<T> streamRequestProcessor;    

    protected final AtomicBoolean isTerminated;

    protected final Publisher<Set<TopicSubscription>> subscriptionActionsFlux;
  }

  @Value
  private static class ConnectedStreamContext<T> {
    protected final ResponseContext<T> responseContext;

    protected final ConcatProcessor<Duration> heartbeatExpectationProcessor;

    protected final ConcatProcessor<Duration> heartbeatSendFrequencyProcessor;

    protected final Duration heartbeatSendFrequency;

    protected final Duration heartbeatReceiveFrequency;

    protected final Flux<T> stream;
  }

  @Value
  private static class SharedStreamContext<T> {
    protected final ConnectedStreamContext<T> connectedStreamContext;

    protected final Flux<T> sharedStream;
  }
}
