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
import reactor.core.Disposable;
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

  private final CacheableResource<SharedStreamContext<T>> sharedStreamContext
      = new CacheableResource<SharedStreamContext<T>>(
          context -> getSharedStreamContext(),
          context -> {
            return !context.getConnectedStreamContext()
            .getResponseContext().getIsTerminated().get(); 
          });

  protected ResponseContext<T> createResponseContext() {
    log.info("Creating response context");
    EmitterProcessor<Long> beforeOpenSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> beforeOpenSignalSink = beforeOpenSignalEmitter.sink();

    EmitterProcessor<Long> disconnectedSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> disconnectedSignalSink = beforeOpenSignalEmitter.sink();

    SwitchableProcessor<Duration> connectionExpectationProcessor =
        SwitchableProcessor.create(beforeOpenSignalEmitter, "beforeOpen");

    SwitchableProcessor<T> streamRequestProcessor =
        SwitchableProcessor.create(beforeOpenSignalEmitter, "streamRequest");


    AtomicBoolean isTerminated = new AtomicBoolean(false);

    return new ResponseContext<>(
        beforeOpenSignalSink,
        disconnectedSignalSink,
        disconnectedSignalEmitter,
        connectionExpectationProcessor,
        streamRequestProcessor,       
        isTerminated);
  }

  protected ConnectedStreamContext<T> createConnectedStreamContext(
      ResponseContext<T> responseContext, Flux<T> source, T connectedFrame) {
    log.info("Creating connected stream context");

    EmitterProcessor<Long> connectedSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> connectedSignalSink = connectedSignalEmitter.sink();

    SwitchableProcessor<Duration> heartbeatSendFrequencyProcessor =
        SwitchableProcessor.create(connectedSignalEmitter, 1, "heartbeatSend");

    SwitchableProcessor<Duration> heartbeatExpectationProcessor =
        SwitchableProcessor.create(connectedSignalEmitter, 1, "heartbeatExpectation");

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

    Flux<T> sourceWithExpectations = source.mergeWith(heartbeatExpectations);

    return new ConnectedStreamContext<>(
        responseContext,
        connectedSignalSink,
        connectedSignalEmitter,
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
                  expectHeartbeatsEvery(
                      context.getHeartbeatExpectationProcessor().sink(), Duration.ZERO);
                  sendHeartbeatsEvery(
                      context.getHeartbeatSendFrequencyProcessor().sink(), Duration.ZERO);
                  sendErrorFrame(
                      error, context.getResponseContext().getStreamRequestProcessor().sink());
                })
            .retryWhen(
                errorFlux ->
                  errorFlux.flatMap(
                    error -> {
                      return Mono.delay(Duration.ofSeconds(5)).doOnNext(delay -> {
                        log.info("Retrying after 5 seconds delay");
                      });
                    }))
            .share()
            .log("sharedStream", Level.INFO);

    TopicRouter<T> topicFluxRouter = TopicRouter.<T>builder()
        .source(sharedStream)
        .startConnected(true)
        .connectedSignal(context.getConnectedSignal()) 
        .disconnectedSignal(context.getResponseContext().getDisconnectedSignal())
        .subscriptionThrottleDuration(subscriptionThrottleDuration)
        .topicFilter(destination -> frame -> isDataFrameForDestination.test(frame, destination))    
        .build();

    Publisher<Set<TopicSubscription>> subscriptions = topicFluxRouter.getSubscriptions();
    Disposable subscriptionsSubscription = Flux.from(subscriptions)
        .doOnNext(topics -> {
          List<T> subscriptionFrames = buildSubscribeFrames.apply(topics);
          subscriptionFrames.stream().forEach(subscriptionFrame -> {
            context.getResponseContext().getStreamRequestProcessor().sink()
                .next(subscriptionFrame);
          });          
        })
        .subscribe();

    Publisher<Set<TopicSubscription>> cancellations = topicFluxRouter.getCancellations();
    Disposable cancellationsSubscription = Flux.from(cancellations)
        .doOnNext(topics -> {
          List<T> unsubscribeFrames = buildUnsubscribeFrames.apply(topics);
          unsubscribeFrames.stream().forEach(unsubscribeFrame -> {
            context.getResponseContext().getStreamRequestProcessor().sink()
                .next(unsubscribeFrame);
          });          
        })
        .subscribe();

    log.info("Returning SharedStreamContext.");
    return new SharedStreamContext<>(
        context,
        topicFluxRouter,
        subscriptionsSubscription,
        cancellationsSubscription);
  }

  protected Mono<SharedStreamContext<T>> getSharedStreamContext() {
    return Mono.<SharedStreamContext<T>, ResponseContext<T>>usingWhen(
        Mono.fromSupplier(this::createResponseContext),
        context -> getSharedStreamContext(context),
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

  protected Mono<SharedStreamContext<T>> getSharedStreamContext(
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
              context.getDisconnectedSignalSink().next(1L);
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

    Mono<T> connectedWithExpectations =
        connected
            .mergeWith(connectionExpectation)            
            .log("sharedConnectedWithExpectations", Level.INFO)
            .share()
            .next()
            .log("connectedWithExpectations", Level.INFO);

    return connectedWithExpectations.<SharedStreamContext<T>>flatMap(
        connectedFrame ->
            Mono.fromSupplier(() -> createConnectedStreamContext(
              context, connected, connectedFrame))
                .log("ConnectedStreamContextSupplier", Level.INFO)
                .<SharedStreamContext<T>>map(
                    connectedStreamContext -> {
                      connectedStreamContext.getConnectedSignalSink().next(1L);

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
      SwitchableProcessor<Duration> connectionExpectationProcessor) {
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
            "Json processing error.\n Message: {0}\nValue: {1}\n",
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
      context -> context.getTopicRouter().get(destination)
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
  private static class ResponseContext<T> {
    protected final FluxSink<Long> beforeOpenSignalSink;

    protected final FluxSink<Long> disconnectedSignalSink;

    protected final Publisher<Long> disconnectedSignal;

    protected final SwitchableProcessor<Duration> connectionExpectationProcessor;

    protected final SwitchableProcessor<T> streamRequestProcessor;    

    protected final AtomicBoolean isTerminated;
  }

  @Value
  private static class ConnectedStreamContext<T> {
    protected final ResponseContext<T> responseContext;

    protected final FluxSink<Long> connectedSignalSink;

    protected final Publisher<Long> connectedSignal;

    protected final SwitchableProcessor<Duration> heartbeatExpectationProcessor;

    protected final SwitchableProcessor<Duration> heartbeatSendFrequencyProcessor;

    protected final Duration heartbeatSendFrequency;

    protected final Duration heartbeatReceiveFrequency;

    protected final Flux<T> stream;
  }

  @Value
  private static class SharedStreamContext<T> {
    protected final ConnectedStreamContext<T> connectedStreamContext;

    protected final TopicRouter<T> topicRouter;

    protected final Disposable subscriptionSubscription;

    protected final Disposable cancellationsSubscription;
  }
}
