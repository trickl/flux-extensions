package com.trickl.flux.websocket;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trickl.exceptions.ConnectionTimeoutException;
import com.trickl.exceptions.MissingHeartbeatException;
import com.trickl.exceptions.NoDataException;
import com.trickl.exceptions.ReceiptTimeoutException;
import com.trickl.exceptions.RemoteStreamException;
import com.trickl.flux.mappers.ExpectedResponseTimeoutFactory;
import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.retry.ExponentialBackoffRetry;
import com.trickl.flux.websocket.stomp.RawStompFluxClient;
import com.trickl.flux.websocket.stomp.StompFrame;
import com.trickl.flux.websocket.stomp.frames.StompReceiptFrame;
import java.net.URI;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.HashMap;
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
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@Log
@Builder
public class RobustWebSocketFluxClient {
  private final WebSocketClient webSocketClient;

  @Getter private final Supplier<URI> transportUriProvider;

  @Builder.Default private Supplier<HttpHeaders> webSocketHeadersProvider = HttpHeaders::new;

  @Builder.Default private Duration disconnectionReceiptTimeout = Duration.ofSeconds(5);

  @Builder.Default private Duration initialRetryDelay = Duration.ofSeconds(1);

  @Builder.Default private Duration retryConsiderationPeriod = Duration.ofSeconds(255);

  @Builder.Default private int maxRetries = 8;

  @Builder.Default private Mono<Void> doBeforeSessionOpen = Mono.empty();

  @Builder.Default private Mono<Void> doAfterSessionClose = Mono.empty();

  @Builder.Default
  private Function<FluxSink<StompFrame>, Duration> doConnect = sink -> Duration.ZERO;

  @Builder.Default private Predicate<StompFrame> isConnectedFrame = frame -> true;

  @Builder.Default private BiPredicate<StompFrame, String> isDataFrameForDestination =
      (frame, destination) -> true;

  @Builder.Default
  private Function<StompFrame, Duration> getHeartbeatSendFrequencyCallback =
      connectedFrame -> Duration.ofSeconds(10);

  @Builder.Default
  private Function<StompFrame, Duration> getHeartbeatReceiveFrequencyCallback =
      connectedFrame -> Duration.ofSeconds(10);

  @Builder.Default
  private Supplier<Optional<StompFrame>> buildDisconnectFrame = () -> Optional.empty();

  @Builder.Default
  private BiFunction<String, String, Optional<StompFrame>> buildSubscribeFrame = 
      (String destination, String subscriptionId) -> Optional.empty();

  @Builder.Default
  private Function<String, Optional<StompFrame>> buildUnsubscribeFrame = 
      (String subscriptionId) -> Optional.empty();

  @Builder.Default
  private Function<Throwable, Optional<StompFrame>> buildErrorFrame = 
      (Throwable error) -> Optional.empty();

  @Builder.Default
      private Function<Long, Optional<StompFrame>> buildHeartbeatFrame = 
          (Long count) -> Optional.empty();

  @Builder.Default
  private Function<StompFrame, Optional<Throwable>> decodeErrorFrame = 
      (StompFrame frame) -> Optional.empty();

  private final AtomicInteger maxSubscriptionNumber = new AtomicInteger(0);

  private final Map<String, String> subscriptionDestinationIdMap = new HashMap<>();

  private static final double HEARTBEAT_PERCENTAGE_TOLERANCE = 1.5;

  protected ResponseContext createResponseContext() {
    log.info("Creating response context");
    EmitterProcessor<Long> beforeOpenSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> beforeOpenSignalSink = beforeOpenSignalEmitter.sink();

    SwitchableProcessor<Duration> connectionExpectationProcessor =
        SwitchableProcessor.create(beforeOpenSignalEmitter);

    SwitchableProcessor<StompFrame> streamRequestProcessor =
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

    return new ResponseContext(
        beforeOpenSignalSink,
        connectedSignalSink,
        requireReceiptSignalSink,
        connectionExpectationProcessor,
        streamRequestProcessor,
        heartbeatExpectationProcessor,
        heartbeatSendFrequencyProcessor,
        receiptExpectationProcessor);
  }

  protected BaseStreamContext createBaseStreamContext(
      ResponseContext responseContext, Flux<StompFrame> source, StompFrame connectedFrame) {
    log.info("Creating base stream context");

    return new BaseStreamContext(
        responseContext,
        getHeartbeatSendFrequencyCallback.apply(connectedFrame),
        getHeartbeatReceiveFrequencyCallback.apply(connectedFrame),
        source);
  }

  protected SharedStreamContext createSharedStreamContext(
      BaseStreamContext context, Consumer<FluxSink<StompFrame>> doAfterSharedSubscribe) {
    log.info("Creating shared stream context");

    Flux<StompFrame> sharedStream =
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
    return new SharedStreamContext(context, sharedStream);
  }

  protected Flux<SharedStreamContext> getSharedStreamContext(
      Consumer<FluxSink<StompFrame>> doAfterSharedSubscribe) {
    return Flux.<SharedStreamContext, ResponseContext>usingWhen(
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

  protected Flux<SharedStreamContext> getSharedStreamContext(
      ResponseContext context, Consumer<FluxSink<StompFrame>> doAfterSharedSubscribe) {
    log.info("Creating base stream context mono");
    Publisher<StompFrame> sendWithResponse =
        Flux.merge(Flux.from(context.getStreamRequestProcessor()))
            .log("sendWithResponse", Level.FINE);

    RawStompFluxClient stompFluxClient =
        RawStompFluxClient.builder()
            .webSocketClient(webSocketClient)
            .transportUriProvider(transportUriProvider)
            .webSocketHeadersProvider(Mono.fromSupplier(webSocketHeadersProvider))
            .doBeforeOpen(
                doBeforeSessionOpen.then(
                    Mono.fromRunnable(() -> context.getBeforeOpenSignalSink().next(1L))))
            .doAfterOpen(sink -> connect(sink, context.getConnectionExpectationProcessor()))
            .doBeforeClose(pub -> Mono.delay(Duration.ofMillis(500)).then())
            .doAfterClose(doAfterSessionClose)
            .build();

    Flux<StompFrame> base =
        Flux.defer(() -> stompFluxClient.get(sendWithResponse))
            .flatMap(new ThrowableMapper<StompFrame, StompFrame>(this::handleErrorFrame))
            .log("sharedBase")
            .share()
            .doOnSubscribe(sub -> log.info("** SUBSCRIBING **" + sub.toString()))
            .doOnCancel(() -> log.info("** CANCEL  **"))
            .log("base", Level.INFO);

    Flux<StompFrame> connected = base.filter(isConnectedFrame).log("connection", Level.INFO);

    Flux<StompFrame> heartbeats =
        Flux.from(context.getHeartbeatSendFrequencyProcessor())
            .<StompFrame>switchMap(
                frequency ->
                    sendHeartbeats(frequency, context.getStreamRequestProcessor().getSink()))
            .log("heartbeats");

    ExpectedResponseTimeoutFactory<StompFrame> heartbeatExpectationFactory =
        ExpectedResponseTimeoutFactory.<StompFrame>builder()
            .isRecurring(true)
            .isResponse(value -> true)
            .timeoutExceptionMapper(
                (error, frequency) ->
                    new MissingHeartbeatException("No heartbeat within " + frequency, error))
            .build();
    Publisher<StompFrame> heartbeatExpectation =
        heartbeatExpectationFactory.apply(context.getHeartbeatExpectationProcessor(), base);

    Flux<StompFrame> heartbeatExpectations = Flux.from(heartbeatExpectation).mergeWith(heartbeats);

    ExpectedResponseTimeoutFactory<StompFrame> receiptExpectationFactory =
        ExpectedResponseTimeoutFactory.<StompFrame>builder()
            .isRecurring(false)
            .isResponse(frame -> frame instanceof StompReceiptFrame)
            .timeoutExceptionMapper(
                (error, period) ->
                    new ReceiptTimeoutException("No receipt within " + period, error))
            .build();

    Publisher<StompFrame> requireReceiptExpectation =
        receiptExpectationFactory.apply(context.getReceiptExpectationProcessor(), base);

    ExpectedResponseTimeoutFactory<StompFrame> connectionExpectationFactory =
        ExpectedResponseTimeoutFactory.<StompFrame>builder()
            .isRecurring(false)
            .isResponse(value -> true)
            .timeoutExceptionMapper(
                (error, period) ->
                    new ConnectionTimeoutException("No connection within " + period, error))
            .build();
    Publisher<StompFrame> connectionExpectation =
        connectionExpectationFactory.apply(context.getConnectionExpectationProcessor(), connected);

    Flux<StompFrame> connectedWithExpectations =
        connected
            .mergeWith(connectionExpectation)
            .mergeWith(heartbeatExpectations)
            .mergeWith(Flux.from(requireReceiptExpectation))
            .log("sharedConnectedWithExpectations", Level.INFO)
            .share()
            .log("connectedWithExpectations", Level.INFO);

    Flux<StompFrame> connectedBase =
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
      FluxSink<StompFrame> streamRequestSink,
      SwitchableProcessor<Duration> connectionExpectationProcessor) {
    Duration connectionTimeout = doConnect.apply(streamRequestSink);
    expectConnectionWithin(connectionExpectationProcessor.getSink(), connectionTimeout);
    return Mono.empty();
  }

  protected Mono<Void> disconnect(
      Publisher<StompFrame> response,
      FluxSink<StompFrame> streamRequestSink,
      FluxSink<Duration> receiptExpectationSink) {
    disconnect(streamRequestSink, receiptExpectationSink);

    return Mono.from(
            Flux.from(response)
                .filter(frame -> StompReceiptFrame.class.equals(frame.getClass()))
                .cast(StompReceiptFrame.class)
                .log("disconnect"))
        .then();
  }

  protected void disconnect(
      FluxSink<StompFrame> streamRequestSink, FluxSink<Duration> receiptExpectationSink) {    
    Optional<StompFrame> disconnectFrame = buildDisconnectFrame.get();
    log.info("Disconnecting...");    
    if (disconnectFrame.isPresent()) {
      receiptExpectationSink.next(disconnectionReceiptTimeout);
      streamRequestSink.next(disconnectFrame.get());
    }
  }

  protected StompFrame handleErrorFrame(StompFrame frame) throws RemoteStreamException {  
    Optional<Throwable> error = decodeErrorFrame.apply(frame);
    if (error.isPresent()) {
      throw new RemoteStreamException("Remote stream encountered error", error.get());
    }
    return frame;
  }

  protected void sendErrorFrame(Throwable error, FluxSink<StompFrame> streamRequestSink) {
    Optional<StompFrame> errorFrame = buildErrorFrame.apply(error);
    if (errorFrame.isPresent()) {
      streamRequestSink.next(errorFrame.get());
    }
  }

  protected Publisher<StompFrame> sendHeartbeats(
      Duration frequency, FluxSink<StompFrame> streamRequestSink) {
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
              return Mono.<StompFrame>empty();
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
      String destination, FluxSink<StompFrame> streamRequestSinkRef) {
    log.info("Subscribing to destination " + destination);
    int subscriptionNumber = maxSubscriptionNumber.getAndIncrement();
    String subscriptionId = MessageFormat.format("sub-{0}", subscriptionNumber);

    Optional<StompFrame> subscribeFrame = 
        buildSubscribeFrame.apply(destination, subscriptionId);
    if (subscribeFrame.isPresent()) {
      streamRequestSinkRef.next(subscribeFrame.get());
    }
    return subscriptionId;
  }

  protected void resubscribeAll(FluxSink<StompFrame> streamRequestSinkRef) {
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
  public Flux<StompFrame> subscribe(
      String destination, Duration minMessageFrequency) {
    // Drain the subscription only once
    Consumer<FluxSink<StompFrame>> doAfterSharedStreamSubscribe =
        sink -> {
          log.info("Subscribing to " + destination);
          subscriptionDestinationIdMap.computeIfAbsent(
              destination, d -> subscribeDestination(d, sink));
        };

    return getSharedStreamContext(doAfterSharedStreamSubscribe)
        .flatMap(
            sharedStreamContext ->
                Flux.<StompFrame, SharedStreamContext>usingWhen(
                    Mono.fromSupplier(() -> sharedStreamContext),
                    context ->
                        subscribe(context, destination, minMessageFrequency)
                            .log("sharedStreamContext", Level.INFO),
                    context -> Mono.empty()))
        .log("outerSubscribe", Level.INFO);
  }

  protected Flux<StompFrame> subscribe(
      SharedStreamContext context,
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

  protected void unsubscribe(String destination, FluxSink<StompFrame> streamRequestSink) {
    subscriptionDestinationIdMap.computeIfPresent(
        destination,
        (dest, subscriptionId) -> {
          Optional<StompFrame> unsubscribeFrame = 
              buildUnsubscribeFrame.apply(subscriptionId);
          if (unsubscribeFrame.isPresent()) {
            streamRequestSink.next(unsubscribeFrame.get());
          }
          return null;
        });
  }

  @Value
  private static class ResponseContext {
    protected final FluxSink<Long> beforeOpenSignalSink;

    protected final FluxSink<Long> connectedSignalSink;

    protected final FluxSink<Long> requireReceiptSignalSink;

    protected final SwitchableProcessor<Duration> connectionExpectationProcessor;

    protected final SwitchableProcessor<StompFrame> streamRequestProcessor;

    protected final SwitchableProcessor<Duration> heartbeatExpectationProcessor;

    protected final SwitchableProcessor<Duration> heartbeatSendFrequencyProcessor;

    protected final SwitchableProcessor<Duration> receiptExpectationProcessor;
  }

  @Value
  private static class BaseStreamContext {
    protected final ResponseContext responseContext;

    protected final Duration heartbeatSendFrequency;

    protected final Duration heartbeatReceiveFrequency;

    protected final Flux<StompFrame> stream;
  }

  @Value
  private static class SharedStreamContext {
    protected final BaseStreamContext baseStreamContext;

    protected final Flux<StompFrame> sharedStream;
  }
}
