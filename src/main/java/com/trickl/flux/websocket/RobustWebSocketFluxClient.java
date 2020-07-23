package com.trickl.flux.websocket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trickl.exceptions.ConnectionTimeoutException;
import com.trickl.exceptions.MissingHeartbeatException;
import com.trickl.exceptions.NoDataException;
import com.trickl.exceptions.ReceiptTimeoutException;
import com.trickl.exceptions.RemoteStreamException;
import com.trickl.flux.mappers.ExpectedResponseTimeoutFactory;
import com.trickl.flux.mappers.FluxSinkAdapter;
import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.mappers.ThrowingFunction;
import com.trickl.flux.retry.ExponentialBackoffRetry;
import com.trickl.flux.websocket.stomp.DecodingTransformer;
import com.trickl.flux.websocket.stomp.EncodingTransformer;
import java.io.IOException;
import java.net.URI;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
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
  private BiFunction<String, String, Optional<T>> buildSubscribeFrame = 
      (String destination, String subscriptionId) -> Optional.empty();

  @Builder.Default
  private Function<String, Optional<T>> buildUnsubscribeFrame = 
      (String subscriptionId) -> Optional.empty();

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
  ThrowingFunction<T, S, IOException> encoder = frame -> null;

  @Builder.Default
  ThrowingFunction<S, List<T>, IOException> decoder = bytes -> Collections.emptyList();

  private final AtomicInteger maxSubscriptionNumber = new AtomicInteger(0);

  private final Map<String, String> subscriptionDestinationIdMap = new HashMap<>();

  private static final double HEARTBEAT_PERCENTAGE_TOLERANCE = 1.5;

  protected ResponseContext<T> createResponseContext() {
    log.info("Creating response context");
    EmitterProcessor<Long> beforeOpenSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> beforeOpenSignalSink = beforeOpenSignalEmitter.sink();

    SwitchableProcessor<Duration> connectionExpectationProcessor =
        SwitchableProcessor.create(beforeOpenSignalEmitter);

    SwitchableProcessor<T> streamRequestProcessor =
        SwitchableProcessor.create(beforeOpenSignalEmitter);

    EmitterProcessor<Long> connectedSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> connectedSignalSink = connectedSignalEmitter.sink();

    SwitchableProcessor<Duration> heartbeatSendFrequencyProcessor =
        SwitchableProcessor.create(connectedSignalEmitter);

    SwitchableProcessor<Duration> heartbeatExpectationProcessor =
        SwitchableProcessor.create(connectedSignalEmitter);

    EmitterProcessor<Long> requireReceiptSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> requireReceiptSignalSink = requireReceiptSignalEmitter.sink();

    SwitchableProcessor<Duration> receiptExpectationProcessor =
        SwitchableProcessor.create(requireReceiptSignalEmitter);

    return new ResponseContext<>(
        beforeOpenSignalSink,
        connectedSignalSink,
        requireReceiptSignalSink,
        connectionExpectationProcessor,
        streamRequestProcessor,
        heartbeatExpectationProcessor,
        heartbeatSendFrequencyProcessor,
        receiptExpectationProcessor);
  }

  protected BaseStreamContext<T> createBaseStreamContext(
      ResponseContext<T> responseContext, Flux<T> source, T connectedFrame) {
    log.info("Creating base stream context");

    return new BaseStreamContext<>(
        responseContext,
        getHeartbeatSendFrequencyCallback.apply(connectedFrame),
        getHeartbeatReceiveFrequencyCallback.apply(connectedFrame),
        source);
  }

  protected SharedStreamContext<T> createSharedStreamContext(
      BaseStreamContext<T> context, Consumer<FluxSink<T>> doAfterSharedSubscribe) {
    log.info("Creating shared stream context");

    Flux<T> sharedStream =
        context
            .getStream()
            // .timeout(Duration.ofSeconds(5))
            .onErrorContinue(JsonProcessingException.class, this::warnAndDropError)
            .doOnSubscribe(
                sub -> {
                  resubscribeAll(
                      context.getResponseContext().getStreamRequestProcessor().getSink());

                  doAfterSharedSubscribe.accept(
                      context.getResponseContext().getStreamRequestProcessor().getSink());
                })
            .doOnSubscribe(
                sub -> {
                  if (!context.getHeartbeatReceiveFrequency().isZero()) {
                    Duration heartbeatExpectation =
                        context
                            .getHeartbeatReceiveFrequency()
                            .plus(
                                context
                                    .getHeartbeatReceiveFrequency()
                                    .multipliedBy((long) (HEARTBEAT_PERCENTAGE_TOLERANCE * 100))
                                    .dividedBy(100));
                    expectHeartbeatsEvery(
                        context.getResponseContext().getHeartbeatExpectationProcessor().getSink(),
                        heartbeatExpectation);
                  }

                  sendHeartbeatsEvery(
                      context.getResponseContext().getHeartbeatSendFrequencyProcessor().getSink(),
                      context.getHeartbeatSendFrequency());
                })
            .doOnError(
                error ->
                    sendErrorFrame(
                        error, context.getResponseContext().getStreamRequestProcessor().getSink()))
            .retryWhen(
                errorFlux ->
                    errorFlux.flatMap(
                        error -> {
                          log.info("Ignoring error class -" + error.getClass());
                          return Mono.empty();
                        }))
            //         Mono.delay(Duration.ofSeconds(25)).doOnNext(delay ->
            //       log.info("Retrying after 25 seconds delay"))))
            //     .onErrorContinue(error -> true,
            //       (error, value) -> log.info("Ignoring error class -" + error.getClass()))
            .share()
            .log("sharedStream", Level.INFO);

    log.info("Returning SharedStreamContext.");
    return new SharedStreamContext<>(context, sharedStream);
  }

  protected Flux<SharedStreamContext<T>> getSharedStreamContext(
      Consumer<FluxSink<T>> doAfterSharedSubscribe) {
    return Flux.<SharedStreamContext<T>, ResponseContext<T>>usingWhen(
        Mono.fromSupplier(this::createResponseContext),
        context -> getSharedStreamContext(context, doAfterSharedSubscribe),
        context -> {
          log.info("Cleaning up response context");
          return Mono.fromRunnable(
              () -> {
                context.getBeforeOpenSignalSink().complete();
                context.getConnectedSignalSink().complete();
                context.getRequireReceiptSignalSink().complete();

                context.getConnectionExpectationProcessor().complete();
                context.getStreamRequestProcessor().complete();
                context.getHeartbeatExpectationProcessor().complete();
                context.getHeartbeatSendFrequencyProcessor().complete();
                context.getReceiptExpectationProcessor().complete();
              });
        })
        .retryWhen(
            ExponentialBackoffRetry.builder()
                .initialRetryDelay(initialRetryDelay)
                .considerationPeriod(retryConsiderationPeriod)
                .maxRetries(maxRetries)
                .name("baseStreamContext")
                .build())
        .log("baseStreamContext");
  }

  protected Flux<SharedStreamContext<T>> getSharedStreamContext(
      ResponseContext<T> context, Consumer<FluxSink<T>> doAfterSharedSubscribe) {
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
            .doBeforeClose(response -> Mono.delay(Duration.ofMillis(500)).then())
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
            .doOnSubscribe(sub -> log.info("** SUBSCRIBING **" + sub.toString()))
            .doOnCancel(() -> log.info("** CANCEL  **"))
            .log("base", Level.INFO);

    Flux<T> connected = base.filter(isConnectedFrame).log("connection", Level.INFO);

    Flux<T> heartbeats =
        Flux.from(context.getHeartbeatSendFrequencyProcessor())
            .<T>switchMap(
                frequency ->
                    sendHeartbeats(frequency, context.getStreamRequestProcessor().getSink()))
            .log("heartbeats");

    ExpectedResponseTimeoutFactory<T> heartbeatExpectationFactory =
        ExpectedResponseTimeoutFactory.<T>builder()
            .isRecurring(true)
            .isResponse(value -> true)
            .timeoutExceptionMapper(
                (error, frequency) ->
                    new MissingHeartbeatException("No heartbeat within " + frequency, error))
            .build();
    Publisher<T> heartbeatExpectation =
        heartbeatExpectationFactory.apply(context.getHeartbeatExpectationProcessor(), base);

    Flux<T> heartbeatExpectations = Flux.from(heartbeatExpectation).mergeWith(heartbeats);

    ExpectedResponseTimeoutFactory<T> receiptExpectationFactory =
        ExpectedResponseTimeoutFactory.<T>builder()
            .isRecurring(false)
            .isResponse(frame -> isReceiptFrame.test(frame))
            .timeoutExceptionMapper(
                (error, period) ->
                    new ReceiptTimeoutException("No receipt within " + period, error))
            .build();

    Publisher<T> requireReceiptExpectation =
        receiptExpectationFactory.apply(context.getReceiptExpectationProcessor(), base);

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
            .mergeWith(heartbeatExpectations)
            .mergeWith(Flux.from(requireReceiptExpectation))
            .log("sharedConnectedWithExpectations", Level.INFO)
            .share()
            .log("connectedWithExpectations", Level.INFO);

    Flux<T> connectedBase =
        base.mergeWith(connectedWithExpectations.filter(value -> false))
            .log("connectedBase", Level.INFO);

    return connectedWithExpectations.flatMap(
        connectedFrame ->
            Mono.fromSupplier(() -> createBaseStreamContext(context, connectedBase, connectedFrame))
                .log("baseStreamContextSupplier", Level.INFO)
                .flatMapMany(
                    baseStreamContext -> {
                      baseStreamContext.getResponseContext().getConnectedSignalSink().next(1L);
                      return Mono.fromSupplier(
                          () ->
                              createSharedStreamContext(baseStreamContext, doAfterSharedSubscribe));
                    },
                    Mono::error,
                    Mono::empty),
        Mono::error,
        Mono::empty);

    /*
    log.info("Cleaning up baseStreamContext");
    baseStreamContext.getResponseContext().getRequireReceiptSignalSink().next(1L);
    return Mono.empty();

    return disconnect(baseStreamContext.getStream(),
    baseStreamContext.getResponseContext().getStreamRequestSinkRef(),
    baseStreamContext.getResponseContext().getReceiptExpectationSinkRef())
    .then(Mono.fromRunnable(() -> {
      // Close socket
    }));
    */
  }

  protected Mono<Void> connect(
      FluxSink<T> streamRequestSink,
      SwitchableProcessor<Duration> connectionExpectationProcessor) {
    Duration connectionTimeout = doConnect.apply(streamRequestSink);
    expectConnectionWithin(connectionExpectationProcessor.getSink(), connectionTimeout);
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
        .startWith(buildHeartbeatFrame.apply(0L))
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

  protected String subscribeDestination(
      String destination, FluxSink<T> streamRequestSinkRef) {
    log.info("Subscribing to destination " + destination);
    int subscriptionNumber = maxSubscriptionNumber.getAndIncrement();
    String subscriptionId = MessageFormat.format("sub-{0}", subscriptionNumber);

    Optional<T> subscribeFrame = 
        buildSubscribeFrame.apply(destination, subscriptionId);
    if (subscribeFrame.isPresent()) {
      streamRequestSinkRef.next(subscribeFrame.get());
    }
    return subscriptionId;
  }

  protected void resubscribeAll(FluxSink<T> streamRequestSinkRef) {
    log.info("Resubscribing to everything");
    subscriptionDestinationIdMap.replaceAll(
        (dest, id) -> subscribeDestination(dest, streamRequestSinkRef));
  }

  /**
   * Subscribe to a destination.
   *
   * @param destination The destination channel
   * @param minMessageFrequency Unsubscribe if no message received in this time
   * @return A flux of messages on that channel
   */
  public Flux<T> subscribe(
      String destination, Duration minMessageFrequency) {
    // Drain the subscription only once
    Consumer<FluxSink<T>> doAfterSharedStreamSubscribe =
        sink -> {
          log.info("Subscribing to " + destination);
          subscriptionDestinationIdMap.computeIfAbsent(
              destination, d -> subscribeDestination(d, sink));
        };

    return getSharedStreamContext(doAfterSharedStreamSubscribe)
        .flatMap(
            sharedStreamContext ->
                Flux.<T, SharedStreamContext<T>>usingWhen(
                    Mono.fromSupplier(() -> sharedStreamContext),
                    context ->
                        subscribe(context, destination, minMessageFrequency)
                            .log("sharedStreamContext", Level.INFO),
                    context -> Mono.empty()))
        .log("outerSubscribe", Level.INFO);
  }

  protected Flux<T> subscribe(
      SharedStreamContext<T> context,
      String destination,
      Duration minMessageFrequency) {
    return context
            .getSharedStream()
            .filter(frame -> isDataFrameForDestination.test(frame, destination))
            .timeout(minMessageFrequency)
            .onErrorMap(
                error -> {
                  if (error instanceof TimeoutException) {
                    return new NoDataException("No data within " + minMessageFrequency, error);
                  }
                  return error;
                })
            .doFinally(
                signal ->
                    unsubscribe(
                        destination,
                        context
                            .getBaseStreamContext()
                            .getResponseContext()
                            .getStreamRequestProcessor()
                            .getSink()))
            .log("innerSubscribe", Level.INFO);
  }

  protected void unsubscribe(String destination, FluxSink<T> streamRequestSink) {
    subscriptionDestinationIdMap.computeIfPresent(
        destination,
        (dest, subscriptionId) -> {
          Optional<T> unsubscribeFrame = 
              buildUnsubscribeFrame.apply(subscriptionId);
          if (unsubscribeFrame.isPresent()) {
            streamRequestSink.next(unsubscribeFrame.get());
          }
          return null;
        });
  }

  @Value
  private static class ResponseContext<T> {
    protected final FluxSink<Long> beforeOpenSignalSink;

    protected final FluxSink<Long> connectedSignalSink;

    protected final FluxSink<Long> requireReceiptSignalSink;

    protected final SwitchableProcessor<Duration> connectionExpectationProcessor;

    protected final SwitchableProcessor<T> streamRequestProcessor;

    protected final SwitchableProcessor<Duration> heartbeatExpectationProcessor;

    protected final SwitchableProcessor<Duration> heartbeatSendFrequencyProcessor;

    protected final SwitchableProcessor<Duration> receiptExpectationProcessor;
  }

  @Value
  private static class BaseStreamContext<T> {
    protected final ResponseContext<T> responseContext;

    protected final Duration heartbeatSendFrequency;

    protected final Duration heartbeatReceiveFrequency;

    protected final Flux<T> stream;
  }

  @Value
  private static class SharedStreamContext<T> {
    protected final BaseStreamContext<T> baseStreamContext;

    protected final Flux<T> sharedStream;
  }
}
