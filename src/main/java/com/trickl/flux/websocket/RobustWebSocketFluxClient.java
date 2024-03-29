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
import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.mappers.ThrowingFunction;
import com.trickl.flux.monitors.SetAction;
import com.trickl.flux.monitors.SetActionType;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@Log
@Builder
public class RobustWebSocketFluxClient<S, T, TopicT> {
  private final WebSocketClient webSocketClient;

  @Getter private final Supplier<URI> transportUriProvider;

  @Getter private final BiFunction<Consumer<S>, Publisher<S>, WebSocketHandler> handlerFactory;

  @Builder.Default
  private Mono<HttpHeaders> webSocketHeadersProvider = Mono.fromSupplier(HttpHeaders::new);

  @Builder.Default private Duration disconnectionReceiptTimeout = Duration.ofSeconds(5);

  @Builder.Default private Duration initialRetryDelay = Duration.ofSeconds(1);

  @Builder.Default private Duration retryConsiderationPeriod = Duration.ofSeconds(255);

  @Builder.Default private int maxRetries = 8;

  @Builder.Default private Mono<Void> doBeforeSessionOpen = Mono.empty();

  @Builder.Default private Mono<Void> doAfterSessionClose = Mono.empty();

  @Builder.Default private Function<Consumer<T>, Duration> doConnect = sink -> Duration.ZERO;

  @Builder.Default private Predicate<T> isConnectedFrame = null;

  @Builder.Default private Predicate<T> isDisconnectReceiptFrame = null;

  @Builder.Default
  private BiPredicate<T, TopicT> isDataFrameForDestination = (frame, destination) -> true;

  @Builder.Default
  private Function<T, Duration> getHeartbeatSendFrequencyCallback =
      connectedFrame -> Duration.ofSeconds(10);

  @Builder.Default
  private Function<T, Duration> getHeartbeatReceiveFrequencyCallback =
      connectedFrame -> Duration.ofSeconds(10);

  @Builder.Default private Supplier<Optional<T>> buildDisconnectFrame = () -> Optional.empty();

  @Builder.Default
  private BiFunction<Set<TopicSubscription<TopicT>>, Set<TopicSubscription<TopicT>>, List<T>>
      buildSubscribeFrames =
          (Set<TopicSubscription<TopicT>> addedTopics, Set<TopicSubscription<TopicT>> allTopics) ->
              Collections.emptyList();

  @Builder.Default
  private BiFunction<Set<TopicSubscription<TopicT>>, Set<TopicSubscription<TopicT>>, List<T>>
      buildUnsubscribeFrames =
          (Set<TopicSubscription<TopicT>> removedTopics,
              Set<TopicSubscription<TopicT>> allTopics) -> Collections.emptyList();

  @Builder.Default
  private Function<Throwable, Optional<T>> buildErrorFrame = (Throwable error) -> Optional.empty();

  @Builder.Default
  private Function<Long, Optional<T>> buildHeartbeatFrame = (Long count) -> Optional.empty();

  @Builder.Default
  private Function<T, Optional<Throwable>> decodeErrorFrame = (T frame) -> Optional.empty();

  @Builder.Default
  private ProtocolFrameHandler<T> handleProtocolFrames =
      (T frame, Sinks.Many<T> sink, Runnable onComplete) -> Mono.just(frame);

  @Builder.Default
  private ThrowingFunction<T, List<S>, IOException> encoder = frame -> Collections.emptyList();

  @Builder.Default
  private ThrowingFunction<S, List<T>, IOException> decoder = bytes -> Collections.emptyList();

  @Builder.Default private Duration subscriptionThrottleDuration = Duration.ofSeconds(1);

  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_RED = "\u001B[31m";

  private final Flux<SharedStreamContext<T, TopicT>> sharedStreamContexts =
      getSharedStreamContexts().cache(1);

  private final CacheableResource<SharedStreamContext<T, TopicT>> sharedStreamContext =
      CacheableResource.<SharedStreamContext<T, TopicT>>builder()
          .resourceGenerator(context -> sharedStreamContexts.next())
          .shouldGenerate(context -> context.getIsTerminated().get())
          .build();

  protected TopicContext<T, TopicT> createTopicContext() {
    log.info("Creating topic context");
    Sinks.Many<ConnectedStreamContext<T, TopicT>> connectedContextSink =
        Sinks.many().multicast().onBackpressureBuffer();

    Sinks.One<Long> disconnectedSignalSink = Sinks.one();

    Publisher<ConnectedStreamContext<T, TopicT>> connectedContexts =
        connectedContextSink
            .asFlux()
            .log("connectedSignal", Level.FINE)
            .doOnNext(value -> log.info(ANSI_RED + "connected" + ANSI_RESET))
            .share();
    Publisher<Long> disconnectedSignal =
        disconnectedSignalSink
            .asMono()
            .log("disconnectedSignal")
            .doOnNext(value -> log.info(ANSI_RED + "disconnected" + ANSI_RESET))
            .share();

    ConcatProcessor<T> streamRequestProcessor = ConcatProcessor.create();

    TopicRouter<T, TopicT> topicRouter =
        TopicRouter.<T, TopicT>builder()
            .startConnected(false)
            .connectedSignal(connectedContexts)
            .disconnectedSignal(disconnectedSignal)
            .subscriptionThrottleDuration(subscriptionThrottleDuration)
            .topicFilter(destination -> frame -> isDataFrameForDestination.test(frame, destination))
            .build();

    return new TopicContext<T, TopicT>(
        connectedContextSink,
        connectedContexts,
        disconnectedSignalSink,
        disconnectedSignal,
        streamRequestProcessor,
        topicRouter);
  }

  protected ResponseContext<T, TopicT> createResponseContext(TopicContext<T, TopicT> topicContext) {
    log.info("Creating response context");
    Sinks.Many<Long> beforeOpenSignalSink = Sinks.many().multicast().onBackpressureBuffer();

    Sinks.Many<T> forceReconnectSignalSink = Sinks.many().multicast().onBackpressureBuffer();

    ConcatProcessor<Duration> connectionExpectationProcessor = ConcatProcessor.create();

    ConcatProcessor<Duration> disconnectReceiptProcessor = ConcatProcessor.create();

    Publisher<SetAction<TopicSubscription<TopicT>>> subscriptionsActionsFlux =
        Flux.from(topicContext.getTopicRouter().getSubscriptionActions())
            .doOnNext(
                action -> {
                  if (action.getType().equals(SetActionType.Add)) {
                    List<T> subscriptionFrames =
                        buildSubscribeFrames.apply(action.getDelta(), action.getSet());
                    subscriptionFrames.stream()
                        .forEach(
                            subscriptionFrame -> {
                              topicContext
                                  .getStreamRequestProcessor()
                                  .sink()
                                  .tryEmitNext(subscriptionFrame);
                            });
                  } else if (action.getType().equals(SetActionType.Remove)) {
                    List<T> unsubscribeFrames =
                        buildUnsubscribeFrames.apply(action.getDelta(), action.getSet());
                    unsubscribeFrames.stream()
                        .forEach(
                            unsubscribeFrame -> {
                              topicContext
                                  .getStreamRequestProcessor()
                                  .sink()
                                  .tryEmitNext(unsubscribeFrame);
                            });
                  }
                })
            .log("subscriptionActions");

    return new ResponseContext<>(
        topicContext,
        beforeOpenSignalSink,
        forceReconnectSignalSink,
        connectionExpectationProcessor,
        disconnectReceiptProcessor,
        subscriptionsActionsFlux);
  }

  protected ConnectedStreamContext<T, TopicT> createConnectedStreamContext(
      ResponseContext<T, TopicT> responseContext, Flux<T> source, T connectedFrame) {
    log.info("Creating connected stream context");

    ConcatProcessor<Duration> heartbeatSendFrequencyProcessor = ConcatProcessor.create(1);

    ConcatProcessor<Duration> heartbeatExpectationProcessor = ConcatProcessor.create(1);

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
                  String errorMessage =
                      MessageFormat.format(
                          "{0} - no heartbeat received for {1}", formatter.format(now), frequency);
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
                    sendHeartbeats(
                        frequency,
                        responseContext.getTopicContext().getStreamRequestProcessor().sink()))
            .log("heartbeats", Level.FINE);

    Flux<T> heartbeatExpectations =
        Flux.from(heartbeatExpectation)
            .log("heartbeatExpectation", Level.FINE)
            .mergeWith(heartbeats)
            .doOnSubscribe(
                sub -> {
                  log.info("Subscribing to heartbeatExpectation");
                });

    Flux<T> sourceWithExpectations =
        source
            .mergeWith(heartbeatExpectations)
            .doOnError(
                MissingHeartbeatException.class,
                error -> {
                  log.info("Source with Expectations forcing reconnect");
                  expectHeartbeatsEvery(heartbeatExpectationProcessor.sink(), Duration.ZERO);
                  sendHeartbeatsEvery(heartbeatSendFrequencyProcessor.sink(), Duration.ZERO);
                })
            .log("sourceWithExpectations", Level.FINE);

    return new ConnectedStreamContext<>(
        responseContext,
        sourceWithExpectations,
        heartbeatExpectationProcessor,
        heartbeatSendFrequencyProcessor,
        heartbeatSendFrequency,
        heartbeatReceiveFrequency);
  }

  protected SharedStreamContext<T, TopicT> createSharedStreamContext(
      TopicContext<T, TopicT> topicContext,
      Flux<ConnectedStreamContext<T, TopicT>> connectedContexts) {
    log.info("Creating shared stream context");

    AtomicBoolean isTerminated = new AtomicBoolean(false);

    Flux<T> sharedStream =
        connectedContexts
            .switchMap(
                context -> {
                  return context
                      .getStream()
                      .onErrorContinue(JsonProcessingException.class, this::warnAndDropError)
                      .doOnError(
                          error -> {
                            sendErrorFrame(
                                error,
                                context
                                    .getResponseContext()
                                    .getTopicContext()
                                    .getStreamRequestProcessor()
                                    .sink());
                          });
                })
            .retryWhen(
                ExponentialBackoffRetry.builder()
                    .initialRetryDelay(initialRetryDelay)
                    .considerationPeriod(retryConsiderationPeriod)
                    .maxRetries(maxRetries)
                    .name("ConnectedStreamContext")
                    .build())
            .log("sharedStream", Level.FINE)
            .publish()
            .refCount(1, Duration.ofSeconds(1));

    log.info("Returning SharedStreamContext.");
    return new SharedStreamContext<>(topicContext, sharedStream, isTerminated);
  }

  protected Flux<SharedStreamContext<T, TopicT>> getSharedStreamContexts() {
    return Mono.fromSupplier(this::createTopicContext)
        .cache()
        .<SharedStreamContext<T, TopicT>>flatMapMany(
            topicContext -> {
              return Flux.<SharedStreamContext<T, TopicT>>defer(
                  () -> {
                    Flux<ConnectedStreamContext<T, TopicT>> connectedContexts =
                        getConnectedStreamContexts(createResponseContext(topicContext))
                            .log("connectedContexts", Level.FINE);
                    return Mono.just(createSharedStreamContext(topicContext, connectedContexts));
                  });
            });
  }

  protected Flux<ConnectedStreamContext<T, TopicT>> getConnectedStreamContexts(
      ResponseContext<T, TopicT> context) {
    log.info("Creating base stream context mono");
    Publisher<T> sendWithResponse =
        Flux.from(context.getTopicContext().getStreamRequestProcessor())
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
            .webSocketHeadersProvider(webSocketHeadersProvider)
            .doBeforeOpen(
                doBeforeSessionOpen.then(
                    Mono.fromRunnable(() -> context.getBeforeOpenSignalSink().tryEmitNext(1L))))
            .doAfterOpen(
                sink ->
                    connect(
                        frame -> {
                          try {
                            encoder.apply(frame).forEach(sink::accept);
                          } catch (IOException ex) {
                            log.log(
                                Level.WARNING, "Encountered error processing after open hook.", ex);
                          }
                        },
                        context.getConnectionExpectationProcessor()))
            .doBeforeClose(
                response ->
                    beforeCloseAction
                        .get()
                        .apply(response)
                        .then(
                            Mono.fromRunnable(
                                () -> {
                                  context
                                      .getTopicContext()
                                      .getDisconnectedSignalSink()
                                      .tryEmitValue(1L);
                                  beforeCloseAction.set(resp -> Mono.empty());
                                })
                                .then(Mono.delay(Duration.ofSeconds(1)))
                                .then()))
            .doAfterClose(doAfterSessionClose)
            .build();

    Flux<T> base =
        Flux.defer(
            () ->
                inputTransformer
                    .apply(webSocketFluxClient.get(outputTransformer.apply(sendWithResponse)))
                    .log("Input Transformer", Level.FINE))
            .flatMap(new ThrowableMapper<T, T>(this::handleErrorFrame))
            .mergeWith(
                Flux.from(context.getSubscriptionActionsFlux()).flatMap(none -> Mono.empty()))
            .flatMap(
                frame ->
                    handleProtocolFrames.apply(
                        frame,
                        context.getTopicContext().getStreamRequestProcessor().sink(),
                        () -> {
                          context.getTopicContext().getStreamRequestProcessor().complete();
                        }))
            .log("sharedBase", Level.FINE)
            .share()
            .log("base", Level.FINE);

    Flux<T> connected =
        isConnectedFrame != null
            ? base.filter(isConnectedFrame).log("connection", Level.INFO)
            : base.next().flux();

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
            .mergeWith(context.getForceReconnectSignalSink().asFlux())
            .log("sharedConnectedWithExpectations", Level.FINE)
            .share()
            .log("connectedWithExpectations", Level.FINE);

    Flux<ConnectedStreamContext<T, TopicT>> connectedContexts =
        connectedWithExpectations.<ConnectedStreamContext<T, TopicT>>flatMap(
            connectedFrame ->
                Mono.fromSupplier(() -> createConnectedStreamContext(context, base, connectedFrame))
                    .log("ConnectedStreamContextSupplier", Level.FINE)
                    .<ConnectedStreamContext<T, TopicT>>map(
                        connectedStreamContext -> {
                          connectedStreamContext
                              .getResponseContext()
                              .getTopicContext()
                              .getConnectedContextSink()
                              .tryEmitNext(connectedStreamContext);

                          beforeCloseAction.set(
                              response ->
                                  disconnect(
                                      inputTransformer.apply(response),
                                      context.getTopicContext().getStreamRequestProcessor().sink(),
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
      Consumer<T> streamRequestSink, ConcatProcessor<Duration> connectionExpectationProcessor) {
    Duration connectionTimeout = doConnect.apply(streamRequestSink);
    expectConnectionWithin(connectionExpectationProcessor.sink(), connectionTimeout);
    return Mono.empty();
  }

  protected Mono<Void> disconnect(
      Publisher<T> response,
      Sinks.Many<T> streamRequestSink,
      Publisher<Duration> disconnectReceiptExpectationPublisher,
      Sinks.Many<Duration> disconnectReceiptExpectationSink) {
    Optional<T> disconnectFrame = buildDisconnectFrame.get();
    log.info("Disconnecting...");
    if (!disconnectFrame.isPresent()) {
      return Mono.empty();
    }

    if (disconnectionReceiptTimeout.isZero()) {
      streamRequestSink.tryEmitNext(disconnectFrame.get());
      return Mono.empty();
    }

    Mono<T> disconnectReceipt =
        isDisconnectReceiptFrame != null
            ? Flux.from(response)
                .filter(isDisconnectReceiptFrame)
                .next()
                .log("disconnectReceipt", Level.FINE)
            : Flux.from(response).next();

    ExpectedResponseTimeoutFactory<T> disconnectReceiptExpectationFactory =
        ExpectedResponseTimeoutFactory.<T>builder()
            .isRecurring(false)
            .isResponse(value -> true)
            .timeoutExceptionMapper(
                (error, period) ->
                    new ReceiptTimeoutException("No disconnect receipt within " + period, error))
            .build();

    Publisher<T> disconnectReceiptExpectation =
        disconnectReceiptExpectationFactory
            .apply(disconnectReceiptExpectationPublisher, disconnectReceipt)
            .log("disconnectReceiptExceptation", Level.FINE);

    return Flux.from(disconnectReceiptExpectation)
        .mergeWith(
            Mono.fromRunnable(
                () -> {
                  disconnectReceiptExpectationSink.tryEmitNext(disconnectionReceiptTimeout);
                  streamRequestSink.tryEmitNext(disconnectFrame.get());
                }))
        .then()
        .log("disconnect", Level.FINE);
  }

  protected T handleErrorFrame(T frame) throws RemoteStreamException {
    Optional<Throwable> error = decodeErrorFrame.apply(frame);
    if (error.isPresent()) {
      throw new RemoteStreamException("Remote stream encountered error", error.get());
    }
    return frame;
  }

  protected void sendErrorFrame(Throwable error, Sinks.Many<T> streamRequestSink) {
    log.info("Sending error frame");
    Optional<T> errorFrame = buildErrorFrame.apply(error);
    if (errorFrame.isPresent()) {
      streamRequestSink.tryEmitNext(errorFrame.get());
    }
  }

  protected Publisher<T> sendHeartbeats(Duration frequency, Sinks.Many<T> streamRequestSink) {
    log.info("Sending heartbeats every " + frequency.toString());
    if (frequency.isZero()) {
      return Flux.empty();
    }

    return Flux.interval(frequency)
        .map(count -> buildHeartbeatFrame.apply(count))
        .flatMap(
            optionalHeartbeat -> {
              if (optionalHeartbeat.isPresent()) {
                streamRequestSink.tryEmitNext(optionalHeartbeat.get());
              }
              return Mono.<T>empty();
            })
        .log("heartbeats", Level.FINE);
  }

  protected void sendHeartbeatsEvery(
      Sinks.Many<Duration> heartbeatSendFrequencySink, Duration frequency) {
    log.info("Request sending heartbeats every " + frequency.toString());
    heartbeatSendFrequencySink.tryEmitNext(frequency);
  }

  protected void expectHeartbeatsEvery(
      Sinks.Many<Duration> heartbeatExpectationSink, Duration frequency) {
    log.info("Expecting heartbeats every " + frequency.toString());
    heartbeatExpectationSink.tryEmitNext(frequency);
  }

  protected void expectConnectionWithin(
      Sinks.Many<Duration> connectionExpectationSink, Duration period) {
    connectionExpectationSink.tryEmitNext(period);
  }

  protected void warnAndDropError(Throwable ex, Object value) {
    log.log(
        Level.WARNING,
        MessageFormat.format("Message: {0}\nValue: {1}\n", new Object[] {ex.getMessage(), value}));
  }

  /**
   * Get a flux for a destination.
   *
   * @param destination The destination channel
   * @param minMessageFrequency Unsubscribe if no message received in this time
   * @param send Messages to send upstream
   * @return A flux of messages on that channel
   */
  public Flux<T> get(TopicT destination, Duration minMessageFrequency, Publisher<T> send) {
    return sharedStreamContext
        .getResource()
        .flatMapMany(
            context -> {
              TopicContext<T, TopicT> topicContext = context.getTopicContext();
              Publisher<T> sendProcessor =
                  Flux.from(send)
                      .doOnNext(
                          message -> {
                            topicContext.getStreamRequestProcessor().sink().tryEmitNext(message);
                          })
                      .ignoreElements();

              return Flux.merge(
                  topicContext.getTopicRouter().route(context.getSharedStream(), destination),
                  sendProcessor);
            })
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
  private static class TopicContext<T, TopicT> {
    protected final Sinks.Many<ConnectedStreamContext<T, TopicT>> connectedContextSink;

    protected final Publisher<ConnectedStreamContext<T, TopicT>> connectedContexts;

    protected final Sinks.One<Long> disconnectedSignalSink;

    protected final Publisher<Long> disconnectedSignal;

    protected final ConcatProcessor<T> streamRequestProcessor;

    protected final TopicRouter<T, TopicT> topicRouter;
  }

  @Value
  private static class ResponseContext<T, TopicT> {
    protected final TopicContext<T, TopicT> topicContext;

    protected final Sinks.Many<Long> beforeOpenSignalSink;

    protected final Sinks.Many<T> forceReconnectSignalSink;

    protected final ConcatProcessor<Duration> connectionExpectationProcessor;

    protected final ConcatProcessor<Duration> disconnectReceiptExpectationProcessor;

    protected final Publisher<SetAction<TopicSubscription<TopicT>>> subscriptionActionsFlux;
  }

  @Value
  private static class ConnectedStreamContext<T, TopicT> {
    protected final ResponseContext<T, TopicT> responseContext;

    protected final Flux<T> stream;

    protected final ConcatProcessor<Duration> heartbeatExpectationProcessor;

    protected final ConcatProcessor<Duration> heartbeatSendFrequencyProcessor;

    protected final Duration heartbeatSendFrequency;

    protected final Duration heartbeatReceiveFrequency;
  }

  @Value
  private static class SharedStreamContext<T, TopicT> {
    protected final TopicContext<T, TopicT> topicContext;

    protected final Flux<T> sharedStream;

    protected final AtomicBoolean isTerminated;
  }

  @FunctionalInterface
  public interface ProtocolFrameHandler<T> {
    Publisher<T> apply(T frame, Sinks.Many<T> sendSink, Runnable onComplete);
  }
}
