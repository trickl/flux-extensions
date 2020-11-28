package com.trickl.flux.websocket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trickl.exceptions.ConnectionTimeoutException;
import com.trickl.exceptions.MissingHeartbeatException;
import com.trickl.exceptions.NoDataException;
import com.trickl.exceptions.ReceiptTimeoutException;
import com.trickl.exceptions.RemoteStreamException;
import com.trickl.flux.mappers.DecodingTransformer;
import com.trickl.flux.mappers.EncodingTransformer;
import com.trickl.flux.mappers.ExpectedResponseTimeoutFactory;
import com.trickl.flux.mappers.FluxSinkAdapter;
import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.mappers.ThrowingFunction;
import com.trickl.flux.publishers.CacheableResource;
import com.trickl.flux.publishers.ConcatProcessor;
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
import java.util.concurrent.atomic.AtomicReference;
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

  @Builder.Default private Predicate<T> isDisconnectReceiptFrame = frame -> true;

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
          context -> getSharedStreamContexts().cache(1).next(),
          context -> {
            return !context.getIsTerminated().get(); 
          });

  protected TopicContext<T> createTopicContext() {
    log.info("Creating topic context");
    EmitterProcessor<ConnectedStreamContext<T>> connectedContextEmitter = EmitterProcessor.create();
    FluxSink<ConnectedStreamContext<T>> connectedContextSink = connectedContextEmitter.sink();

    EmitterProcessor<Long> disconnectedSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> disconnectedSignalSink = disconnectedSignalEmitter.sink();

    Publisher<ConnectedStreamContext<T>> connectedContexts = connectedContextEmitter
        .log("connectedSignal")
        .doOnNext(value -> log.info(ANSI_RED + "connected" + ANSI_RESET)).share();
    Publisher<Long> disconnectedSignal = disconnectedSignalEmitter
        .log("disconnectedSignal")
        .doOnNext(value -> log.info(ANSI_RED + "disconnected" + ANSI_RESET)).share();

    TopicRouter<T> topicRouter = TopicRouter.<T>builder()
        .startConnected(false)
        .connectedSignal(connectedContexts) 
        .disconnectedSignal(disconnectedSignal)
        .subscriptionThrottleDuration(subscriptionThrottleDuration)
        .topicFilter(destination -> frame -> isDataFrameForDestination.test(frame, destination))    
        .build();

    return new TopicContext<T>(
      connectedContextSink,
      connectedContexts,
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

    ConcatProcessor<Duration> connectionExpectationProcessor =
        ConcatProcessor.create();

    ConcatProcessor<Duration> disconnectReceiptProcessor =
        ConcatProcessor.create();

    ConcatProcessor<T> streamRequestProcessor =
        ConcatProcessor.create();
    
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
        disconnectReceiptProcessor,
        streamRequestProcessor,
        Flux.merge(subscribeActionFlux, unsubscribeActionFlux));
  }

  protected ConnectedStreamContext<T> createConnectedStreamContext(
      ResponseContext<T> responseContext, Flux<T> source, T connectedFrame) {
    log.info("Creating connected stream context");

    ConcatProcessor<Duration> heartbeatSendFrequencyProcessor =
          ConcatProcessor.create(1);

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
        .log("heartbeatExpectation").mergeWith(heartbeats)
        .doOnSubscribe(sub -> {
          log.info("Subscribing to heartbeatExpectation");
        });

    Flux<T> sourceWithExpectations = source
        .mergeWith(heartbeatExpectations)               
        .doOnError(MissingHeartbeatException.class, error -> {
          log.info("Source with Expectations forcing reconnect");  
          expectHeartbeatsEvery(
              heartbeatExpectationProcessor.sink(), Duration.ZERO);
          sendHeartbeatsEvery(
              heartbeatSendFrequencyProcessor.sink(), Duration.ZERO);
        }).log("sourceWithExpectations");
          
    return new ConnectedStreamContext<>(
        responseContext,
        sourceWithExpectations,
        heartbeatExpectationProcessor,
        heartbeatSendFrequencyProcessor,
        heartbeatSendFrequency,
        heartbeatReceiveFrequency);
  }

  protected SharedStreamContext<T> createSharedStreamContext(
      TopicContext<T> topicContext, Flux<ConnectedStreamContext<T>> connectedContexts) {
    log.info("Creating shared stream context");

    AtomicBoolean isTerminated = new AtomicBoolean(false);

    Flux<T> sharedStream =  connectedContexts.switchMap(context -> {
      return context
            .getStream()
            .onErrorContinue(JsonProcessingException.class, this::warnAndDropError)
            .doOnError(
                error -> {
                  sendErrorFrame(
                      error, context.getResponseContext().getStreamRequestProcessor().sink());
                });
    })
        .retryWhen(ExponentialBackoffRetry.builder()
            .initialRetryDelay(initialRetryDelay)
            .considerationPeriod(retryConsiderationPeriod)
            .maxRetries(maxRetries)
            .name("ConnectedStreamContext")
            .build())
        .log("sharedStream", Level.INFO)
        .share();

    log.info("Returning SharedStreamContext.");
    return new SharedStreamContext<>(topicContext, sharedStream, isTerminated);
  }

  protected Flux<SharedStreamContext<T>> getSharedStreamContexts() {
    return Mono.fromSupplier(this::createTopicContext).cache()
        .<SharedStreamContext<T>>flatMapMany(
          topicContext -> {
            return Flux.<SharedStreamContext<T>>defer(() -> {
              Flux<ConnectedStreamContext<T>> connectedContexts = 
                  getConnectedStreamContexts(createResponseContext(topicContext));
              return Mono.just(createSharedStreamContext(topicContext, connectedContexts));
            });            
          });
  }

  protected Flux<ConnectedStreamContext<T>> getConnectedStreamContexts(
      ResponseContext<T> context) {
    log.info("Creating base stream context mono");
    Publisher<T> sendWithResponse =
        Flux.merge(Flux.from(context.getStreamRequestProcessor()))
            .log("sendWithResponse", Level.FINE);


    DecodingTransformer<S, T> inputTransformer = new DecodingTransformer<S, T>(decoder);
    EncodingTransformer<T, S> outputTransformer = new EncodingTransformer<T, S>(encoder);
    AtomicReference<Function<Publisher<S>, Mono<Void>>> beforeCloseAction = 
        new AtomicReference<>(response -> Mono.empty());

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
            .doBeforeClose(response -> 
              beforeCloseAction.get().apply(response)
                  .then(Mono.fromRunnable(() -> {
                    context.getTopicContext().getDisconnectedSignalSink().next(1L);
                    beforeCloseAction.set(resp -> Mono.empty());
                  })))
            .doAfterClose(doAfterSessionClose)
            .build();

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
                (error, period) -> {
                    log.info("Creating connection timeout exception.");       
                    return new ConnectionTimeoutException("No connection within " + period, error);
                })
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

    Flux<ConnectedStreamContext<T>> connectedContexts =
        connectedWithExpectations.<ConnectedStreamContext<T>>flatMap(
          connectedFrame ->
            Mono.fromSupplier(() -> createConnectedStreamContext(
              context, base, connectedFrame))
                .log("ConnectedStreamContextSupplier", Level.INFO)
                .<ConnectedStreamContext<T>>map(
                    connectedStreamContext -> {
                      connectedStreamContext
                          .getResponseContext()
                          .getTopicContext()
                          .getConnectedContextSink()
                          .next(connectedStreamContext);

                    beforeCloseAction.set(response ->
                        disconnect(
                          inputTransformer.apply(response), 
                          context.getStreamRequestProcessor().sink(), 
                          context.getDisconnectReceiptExpectationProcessor(),
                          context.getDisconnectReceiptExpectationProcessor().sink()));

                      log.info("Sending / expecting heartbeats");
                      if (!connectedStreamContext.getHeartbeatReceiveFrequency().isZero()) {
                        expectHeartbeatsEvery(
                            connectedStreamContext.getHeartbeatExpectationProcessor().sink(),
                            connectedStreamContext.getHeartbeatReceiveFrequency());
                      }

                      sendHeartbeatsEvery(
                          connectedStreamContext.getHeartbeatSendFrequencyProcessor().sink(),
                          connectedStreamContext.getHeartbeatSendFrequency());
                          
                      return connectedStreamContext;
                    }));

    return connectedContexts;
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
      Publisher<Duration> disconnectReceiptExpectationProcessor,
      FluxSink<Duration> disconnectReceiptExpectationSink) {
    Optional<T> disconnectFrame = buildDisconnectFrame.get();
    log.info("Disconnecting...");
    if (!disconnectFrame.isPresent()) {      
      return Mono.empty();      
    }

    if (disconnectionReceiptTimeout.isZero()) {
      streamRequestSink.next(disconnectFrame.get());
      return Mono.empty();
    }
    
    Mono<T> disconnectReceipt = Flux.from(response)
        .filter(isDisconnectReceiptFrame)
        .next()
        .log("disconnectReceipt", Level.INFO);

    ExpectedResponseTimeoutFactory<T> disconnectReceiptExpectationFactory =
        ExpectedResponseTimeoutFactory.<T>builder()
            .isRecurring(false)
            .isResponse(value -> true)
            .timeoutExceptionMapper(
                (error, period) ->
                    new ReceiptTimeoutException("No disconnect receipt within " + period, error))
            .build();

    Publisher<T> disconnectReceiptExpectation =
        disconnectReceiptExpectationFactory.apply(
          disconnectReceiptExpectationProcessor, disconnectReceipt)
          .log("disconnectReceiptExceptation");

    return Flux.from(disconnectReceiptExpectation).mergeWith(
      Mono.fromRunnable(() -> {        
        disconnectReceiptExpectationSink.next(disconnectionReceiptTimeout);
        streamRequestSink.next(disconnectFrame.get());
      })).then().log("disconnect");
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
    return sharedStreamContext.getResource().flatMapMany(
      context -> context
          .getTopicContext()
          .getTopicRouter()
          .route(
          context.getSharedStream(), destination)
    )
    .timeout(minMessageFrequency)
    .onErrorMap(
      error -> {
        if (error instanceof TimeoutException) {
          return new NoDataException("No data within " + minMessageFrequency, error);
        }
        return error;
      });      
  }

  @Value
  private static class TopicContext<T> {
    protected final FluxSink<ConnectedStreamContext<T>> connectedContextSink;

    protected final Publisher<ConnectedStreamContext<T>> connectedContexts;

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

    protected final ConcatProcessor<Duration> disconnectReceiptExpectationProcessor;

    protected final ConcatProcessor<T> streamRequestProcessor;    

    protected final Publisher<Set<TopicSubscription>> subscriptionActionsFlux;
  }

  @Value
  private static class ConnectedStreamContext<T> {
    protected final ResponseContext<T> responseContext;

    protected final Flux<T> stream;

    protected final ConcatProcessor<Duration> heartbeatExpectationProcessor;

    protected final ConcatProcessor<Duration> heartbeatSendFrequencyProcessor;

    protected final Duration heartbeatSendFrequency;

    protected final Duration heartbeatReceiveFrequency;
  }

  @Value
  private static class SharedStreamContext<T> {    
    protected final TopicContext<T> topicContext;

    protected final Flux<T> sharedStream;

    protected final AtomicBoolean isTerminated;
  }
}
