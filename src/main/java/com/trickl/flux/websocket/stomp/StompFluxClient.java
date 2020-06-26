package com.trickl.flux.websocket.stomp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.exceptions.ConnectionTimeoutException;
import com.trickl.exceptions.MissingHeartbeatException;
import com.trickl.exceptions.NoDataException;
import com.trickl.exceptions.ReceiptTimeoutException;
import com.trickl.exceptions.RemoteStreamException;
import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.mappers.WarnOnErrorMapper;
import com.trickl.flux.retry.ExponentialBackoffRetry;
import com.trickl.flux.websocket.stomp.frames.StompConnectFrame;
import com.trickl.flux.websocket.stomp.frames.StompConnectedFrame;
import com.trickl.flux.websocket.stomp.frames.StompDisconnectFrame;
import com.trickl.flux.websocket.stomp.frames.StompErrorFrame;
import com.trickl.flux.websocket.stomp.frames.StompHeartbeatFrame;
import com.trickl.flux.websocket.stomp.frames.StompMessageFrame;
import com.trickl.flux.websocket.stomp.frames.StompReceiptFrame;
import com.trickl.flux.websocket.stomp.frames.StompSubscribeFrame;
import com.trickl.flux.websocket.stomp.frames.StompUnsubscribeFrame;
import java.io.IOException;
import java.net.URI;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Level;
import lombok.Builder;
import lombok.Data;
import lombok.Value;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@Log
@Builder
public class StompFluxClient {
  private final WebSocketClient webSocketClient;
  private final Supplier<URI> transportUriProvider;

  @Builder.Default private final ObjectMapper objectMapper = new ObjectMapper();

  @Builder.Default private Supplier<HttpHeaders> webSocketHeadersProvider = HttpHeaders::new;

  @Builder.Default private Duration heartbeatSendFrequency = Duration.ofSeconds(5);

  @Builder.Default private Duration heartbeatReceiveFrequency = Duration.ofSeconds(5);

  @Builder.Default private Duration connectionTimeout = Duration.ofSeconds(3);

  @Builder.Default private Duration disconnectionReceiptTimeout = Duration.ofSeconds(5);

  @Builder.Default private  Duration initialRetryDelay = Duration.ofSeconds(1);
  
  @Builder.Default private  Duration retryConsiderationPeriod = Duration.ofSeconds(255);
  
  @Builder.Default private  int maxRetries = 8;

  @Builder.Default private Mono<Void> doBeforeSessionOpen = Mono.empty();

  @Builder.Default private Mono<Void> doAfterSessionClose = Mono.empty();

  private final AtomicInteger maxSubscriptionNumber = new AtomicInteger(0);

  private final Map<String, String> subscriptionDestinationIdMap = new HashMap<>();

  private static final double HEARTBEAT_PERCENTAGE_TOLERANCE = 1.5;

  protected Mono<Void> disconnectStream(SharedStreamContext context) {
    context.getHeartbeatExpectationSink().complete();
    return Mono.empty();
  }

  protected <T> Flux<T> createSwitchablePublisherAndSink(
      Publisher<Long> beforeOpenSignal, FluxSinkRef<T> sinkRef, String name) {
    log.info("Calling switchable publisher and sink for " + name);
    Flux<Publisher<T>> mergedPublishers = Flux.from(beforeOpenSignal).map(signal -> {
      log.info("Creating switchable publisher and sink " + name);
      EmitterProcessor<T> nextEmitter =  EmitterProcessor.<T>create();
      sinkRef.setSink(nextEmitter.sink());
      return nextEmitter;
    });

    return Flux.switchOnNext(mergedPublishers);
  }

  protected ResponseContext createResponseContext(Publisher<Long> beforeOpenSignal) {
    log.info("Creating response context");
    FluxSinkRef<Duration> connectionExpectationSinkRef = new FluxSinkRef<>();
    FluxSinkRef<StompFrame> streamRequestSinkRef = new FluxSinkRef<>();

    Flux<Duration> connectionExpectationFlux
        = createSwitchablePublisherAndSink(
          beforeOpenSignal, connectionExpectationSinkRef, "connectionExpectation").share();
    Disposable connectionSubscription = connectionExpectationFlux.subscribe();

    Flux<StompFrame> streamRequestFlux
        = createSwitchablePublisherAndSink(
          beforeOpenSignal, streamRequestSinkRef, "streamRequest").share().cache(1)
        .log("streamRequest");
    Disposable streamRequestSubscription = streamRequestFlux.subscribe();

    return new ResponseContext(
      connectionExpectationFlux, 
      connectionExpectationSinkRef,
      connectionSubscription,
      streamRequestFlux,
      streamRequestSinkRef,
      streamRequestSubscription);
  }

  protected Flux<BaseStreamContext> getBaseStreamContext() {
    EmitterProcessor<Long> beforeOpenSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> beforeOpenSignalSink = beforeOpenSignalEmitter.sink();
    ResponseContext responseContext = createResponseContext(
        beforeOpenSignalEmitter.log("beforeOpenSignal"));
    return Flux.<BaseStreamContext, ResponseContext>usingWhen(
      Mono.just(responseContext),
      context -> getBaseStreamContext(context, beforeOpenSignalSink),
      context -> {
        log.info("Cleaning up response context");   
        return Mono.create(sink -> {
          context.getConnectionExpectationSinkRef().getSink().complete();
          context.getStreamRequestSinkRef().getSink().complete();
          context.getConnectionExpectationSubscription().dispose();
          context.getStreamRequestSubscription().dispose();
          sink.success();
        });
      }
    )
    .retryWhen(new ExponentialBackoffRetry(
          initialRetryDelay, retryConsiderationPeriod, maxRetries))
    .log("baseStreamContext");    
  }

  protected Flux<BaseStreamContext> getBaseStreamContext(
      ResponseContext context, FluxSink<Long> beforeOpenSinkSink) {
    log.info("Creating base stream context");
    Publisher<StompFrame> sendWithResponse = Flux.merge(
        Flux.from(context.getStreamRequestProcessor()))
        .onErrorMap(new WarnOnErrorMapper())
        .log("sendWithResponse", Level.FINE);

    RawStompFluxClient stompFluxClient =
        RawStompFluxClient.builder()        
            .webSocketClient(webSocketClient)
            .transportUriProvider(transportUriProvider)
            .webSocketHeadersProvider(Mono.fromSupplier(webSocketHeadersProvider))
            .heartbeatSendFrequency(heartbeatSendFrequency)
            .heartbeatReceiveFrequency(heartbeatReceiveFrequency)
            .doBeforeOpen(doBeforeSessionOpen.then(Mono.fromRunnable(() -> {
              beforeOpenSinkSink.next(1L);
            })))
            .doAfterOpen(connect(
              context.getStreamRequestSinkRef(),
              context.getConnectionExpectationSinkRef()))
            .doBeforeClose(pub -> Mono.delay(Duration.ofMillis(500)).then())
            .doAfterClose(doAfterSessionClose)
            .build();

    Flux<StompFrame> base = Flux.defer(() -> stompFluxClient.get(sendWithResponse))
          .flatMap(new ThrowableMapper<StompFrame, StompFrame>(
              this::handleErrorFrame)) 
          .share()
          .log("base", Level.INFO);
    
    Flux<StompConnectedFrame> connected = base.filter(
        frame -> StompConnectedFrame.class.equals(frame.getClass()))
        .mergeWith(Flux.defer(() -> createConnectionExpectation(
            context.getConnectionExpectationProcessor(), base)
            ))
        .cast(StompConnectedFrame.class)        
        .log("connection", Level.INFO);

    return Flux.<BaseStreamContext, StompConnectedFrame>usingWhen(
      connected,
      connectedFrame -> Flux.create(sink -> 
        sink.next(new BaseStreamContext(connectedFrame, base, context.getStreamRequestSinkRef()))
      ),
      connectedFrame -> {
        log.info("No longer connected.");
        return Mono.empty();
      }
    );    
  }
  
  protected Flux<SharedStreamContext> connectStream() {
    return getBaseStreamContext().flatMap(baseStreamContext -> 
      usingConnectStream(baseStreamContext).log("connectStream")   
    );
  }

  protected Flux<SharedStreamContext> usingConnectStream(BaseStreamContext baseStreamContext) {
    return Flux.<SharedStreamContext, BaseStreamContext>usingWhen(
        Mono.just(baseStreamContext),
        context -> Flux.create(sink -> sink.next(connectStreamImpl(context))),
        context -> {
          log.info("Cleaning up connection");
          return disconnect(context.getBase(), context.getStreamRequestSinkRef());
        }
      );
  }

  protected SharedStreamContext connectStreamImpl(BaseStreamContext context) {
    log.info("Creating shared stream context");
    EmitterProcessor<Duration> heartbeatSendProcessor = EmitterProcessor.create();
    Flux<StompFrame> heartbeats = heartbeatSendProcessor.switchMap(
        frequency -> sendHeartbeats(frequency, context.getStreamRequestSinkRef()));
    FluxSink<Duration> heartbeatSendSink = heartbeatSendProcessor.sink();

    EmitterProcessor<Duration> heartbeatExpectationProcessor = EmitterProcessor.create();
    FluxSink<Duration> heartbeatExpectationSink = heartbeatExpectationProcessor.sink();

    log.info("Handling stream connected.");
    handleStreamConnected(
        context.getConnectedFrame().getHeartbeatSendFrequency(),
        context.getConnectedFrame().getHeartbeatReceiveFrequency(),
        heartbeatExpectationSink,
        context.getStreamRequestSinkRef(),
        heartbeatSendSink);

    Flux<StompFrame> sharedStream = context.getBase()                  
        .mergeWith(Flux.defer(() -> createHeartbeatExpectation(
            heartbeatExpectationProcessor, context.getBase())))
        .mergeWith(heartbeats)   
        .onErrorContinue(JsonProcessingException.class, this::warnAndDropError)
        .onErrorMap(new WarnOnErrorMapper())
        .doOnError(error -> sendErrorFrame(error, context.getStreamRequestSinkRef()))
        .retryWhen(new ExponentialBackoffRetry(
          initialRetryDelay, retryConsiderationPeriod, maxRetries))
        .share()
        .log("sharedStream", Level.INFO);

    log.info("Returning SharedStreamContext.");
    return new SharedStreamContext(
      sharedStream, 
      context.getStreamRequestSinkRef(),
      heartbeatExpectationSink);
  }

  protected <T> Publisher<T> createConnectionExpectation(
      Publisher<Duration> connectionExpectationPublisher, Publisher<T> stream) {
    log.info("Creating connection expectation.");
    return Flux.from(connectionExpectationPublisher)
        .switchMap(period -> timeoutNoConnection(period, stream));
  }

  protected <T> Publisher<T> createHeartbeatExpectation(
      Publisher<Duration> heartbeatExpectationPublisher, Publisher<T> stream) {
    return Flux.from(heartbeatExpectationPublisher)
        .switchMap(period -> timeoutNoHeartbeat(period, stream));    
  }

  protected <T> Publisher<T> createReceiptExpectation(
      Publisher<Duration> receiptExpectationPublisher, Publisher<T> stream) {
    log.info("Creating receipt expectation.");
    return Flux.from(receiptExpectationPublisher)
        .switchMap(period -> timeoutNoReceipt(period, stream));
  }

  protected void handleStreamConnected(
      Duration heartbeatSendFrequency,
      Duration heartbeatReceiveFrequency,
      FluxSink<Duration> heartbeatExpectationSink,
      FluxSinkRef<StompFrame> streamRequestSinkRef,
      FluxSink<Duration> heartbeatSendSink) {
    if (!heartbeatSendFrequency.isZero()) {
      Duration heartbeatExpectation = 
          heartbeatSendFrequency.plus(
            heartbeatSendFrequency
              .multipliedBy((long) (HEARTBEAT_PERCENTAGE_TOLERANCE * 100))
              .dividedBy(100));
      expectHeartbeatsEvery(heartbeatExpectationSink, heartbeatExpectation);
    }
    sendHeartbeatsEvery(heartbeatReceiveFrequency, heartbeatSendSink);
    resubscribeAll(streamRequestSinkRef);
  }


  protected Mono<Void> connect(
      FluxSinkRef<StompFrame> streamRequestSinkRef,
      FluxSinkRef<Duration> connectionExpectationSinkRef) {
    return Mono.defer(() -> {
      log.info("Sending connect frame after connection");
      StompConnectFrame connectFrame =
          StompConnectFrame.builder()
              .acceptVersion("1.0,1.1,1.2")
              .heartbeatSendFrequency(heartbeatSendFrequency)
              .heartbeatReceiveFrequency(heartbeatReceiveFrequency)
              .host(transportUriProvider.get().getHost())
              .build();
      streamRequestSinkRef.getSink().next(connectFrame);
      connectionExpectationSinkRef.getSink().next(connectionTimeout);

      return Mono.empty();
    });
  }

  protected void disconnect(
      FluxSinkRef<StompFrame> streamRequestSinkRef,
      FluxSink<Duration> receiptExpectationSink) {
    StompDisconnectFrame disconnectFrame = StompDisconnectFrame.builder().build();
    log.info("Disconnecting...");
    streamRequestSinkRef.getSink().next(disconnectFrame);
    receiptExpectationSink.next(disconnectionReceiptTimeout);
    receiptExpectationSink.complete();
  }

  protected Mono<Void> disconnect(
      Publisher<StompFrame> response, 
      FluxSinkRef<StompFrame> streamRequestSinkRef) {
    EmitterProcessor<Duration> receiptExpectationProcessor = EmitterProcessor.create();
    FluxSink<Duration> receiptExpectationSink = receiptExpectationProcessor.sink();
    disconnect(streamRequestSinkRef, receiptExpectationSink);
    
    return Flux.from(response).filter(
      frame -> StompReceiptFrame.class.equals(frame.getClass()))
      .mergeWith(Flux.defer(() -> createReceiptExpectation(
          receiptExpectationProcessor, response)))        
    .cast(StompReceiptFrame.class).log("disconnect").then();
  }

  protected StompFrame handleErrorFrame(StompFrame frame) throws RemoteStreamException {
    if (StompErrorFrame.class.equals(frame.getClass())) {
      throw new RemoteStreamException(((StompErrorFrame) frame).getMessage());
    }
    return frame;
  }

  protected void sendErrorFrame(Throwable error, FluxSinkRef<StompFrame> streamRequestSinkRef) {
    StompFrame frame = StompErrorFrame.builder().message(error.toString()).build();
    streamRequestSinkRef.getSink().next(frame);
  }

  protected Publisher<StompHeartbeatFrame> sendHeartbeats(
      Duration frequency, FluxSinkRef<StompFrame> streamRequestSinkRef) {
    if (frequency.isZero()) {
      return Flux.empty();
    }

    return Flux.interval(frequency)
        .map(count -> new StompHeartbeatFrame())
        .startWith(new StompHeartbeatFrame())
        .flatMap(heartbeat -> {
          streamRequestSinkRef.getSink().next(heartbeat);
          return Mono.empty();
        });
  }

  protected <T> Publisher<T> timeoutNoHeartbeat(Duration frequency, Publisher<T> stream) {
    if (frequency.isZero()) {
      log.info("Cancelling heartbeat expectation");
      return Flux.empty();
    }
    log.info("Expecting heartbeats every " + frequency.toString());

    return Flux.from(stream)
      .timeout(frequency)
      .onErrorMap(
          error -> {
            if (error instanceof TimeoutException) {
              String message = MessageFormat.format("No heartbeat within {0}", frequency);
              log.log(Level.WARNING, message, error);
              return new MissingHeartbeatException("No heartbeat within " + frequency, error);
            }
            return error;
          })
      .ignoreElements();
  }

  protected <T> Publisher<T> timeoutNoConnection(
      Duration period, Publisher<T> stream) {    
    return Flux.from(stream)
      .doOnSubscribe(sub -> log.info("Expecting a connection within " + period.toString()))
      .timeout(Mono.delay(period))
      .onErrorMap(
          error -> {
            if (error instanceof TimeoutException) {
              String message = MessageFormat.format("No connection within {0}", period);
              log.log(Level.WARNING, message);
              return new ConnectionTimeoutException("No connection within " + period, error);
            }
            return error;
          })
      .ignoreElements();
  }

  protected <T> Publisher<T> timeoutNoReceipt(
      Duration period, Publisher<T> stream) {    
    return Flux.from(stream)
      .doOnSubscribe(sub -> log.info("Expecting a receipt within " + period.toString()))
      .timeout(Mono.delay(period))
      .onErrorMap(
          error -> {
            if (error instanceof TimeoutException) {
              String message = MessageFormat.format("No receipt within {0}", period);
              log.log(Level.WARNING, message);
              return new ReceiptTimeoutException("No receipt within " + period, error);
            }
            return error;
          })
      .ignoreElements();
  }

  protected void sendHeartbeatsEvery(Duration frequency, FluxSink<Duration> heartbeatSendSink) {
    log.info("Sending heartbeats every " + frequency.toString());
    heartbeatSendSink.next(frequency);
  }

  protected void expectHeartbeatsEvery(
      FluxSink<Duration> heartbeatExpectationSink, Duration frequency) {    
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
      String destination, FluxSinkRef<StompFrame> streamRequestSinkRef) {
    int subscriptionNumber = maxSubscriptionNumber.getAndIncrement();
    String subscriptionId = MessageFormat.format("sub-{0}", subscriptionNumber);
    StompFrame frame =
        StompSubscribeFrame.builder()
            .destination(destination)
            .subscriptionId(subscriptionId)
            .build();
    streamRequestSinkRef.getSink().next(frame);
    return subscriptionId;
  }

  protected void resubscribeAll(FluxSinkRef<StompFrame> streamRequestSinkRef) {
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
  public <T> Flux<T> subscribe(
      String destination, Class<T> messageType, Duration minMessageFrequency) {
    return connectStream().flatMap(sharedStreamContext -> 
      Flux.<T, SharedStreamContext>usingWhen(
          Mono.just(sharedStreamContext),
          context -> subscribe(
            context, destination, messageType, minMessageFrequency)
              .log("sharedStreamContext", Level.INFO),
          this::disconnectStream)
    )    
    .onErrorMap(new WarnOnErrorMapper())
    .log("outerSubscribe", Level.INFO);
  }

  protected <T> Flux<T> subscribe(SharedStreamContext context, 
      String destination, Class<T> messageType, Duration minMessageFrequency) {    
    Flux<StompMessageFrame> messageFrameFlux =
        context.getSource()
        .filter(frame -> frame instanceof StompMessageFrame)
        .cast(StompMessageFrame.class)
        .filter(frame -> frame.getDestination().equals(destination))
        .timeout(minMessageFrequency)
        .onErrorMap(
            error -> {
              if (error instanceof TimeoutException) {
                return new NoDataException("No data within " + minMessageFrequency, error);
              }
              return error;
            })
        .doOnSubscribe(sub -> {
          log.info("Subscribing to " + destination);
          subscriptionDestinationIdMap.computeIfAbsent(destination, 
              d -> subscribeDestination(d, context.getRequestSinkRef()));
        })
        .onErrorMap(new WarnOnErrorMapper())
        .doFinally(signal -> unsubscribe(destination, context.getRequestSinkRef()))
        .log("innerSubscribe", Level.INFO);

    return messageFrameFlux.flatMap(
      new ThrowableMapper<>(frame -> readStompMessageFrame(frame, messageType)));
  }

  protected void unsubscribe(String destination, FluxSinkRef<StompFrame> streamRequestSinkRef) {
    subscriptionDestinationIdMap.computeIfPresent(
        destination,
        (dest, subscriptionId) -> {
          StompFrame frame = StompUnsubscribeFrame.builder().subscriptionId(subscriptionId).build();
          streamRequestSinkRef.getSink().next(frame);
          return null;
        });
  }

  protected <T> T readStompMessageFrame(StompMessageFrame frame, Class<T> messageType)
      throws IOException {
    return objectMapper.readValue(frame.getBody(), messageType);
  }

  @Data
  private static class FluxSinkRef<T> {
    protected FluxSink<T> sink;
  }

  @Value
  private static class ResponseContext {
    protected final Publisher<Duration> connectionExpectationProcessor;

    protected final FluxSinkRef<Duration> connectionExpectationSinkRef;

    protected final Disposable connectionExpectationSubscription;
    
    protected final Publisher<StompFrame> streamRequestProcessor;

    protected final FluxSinkRef<StompFrame> streamRequestSinkRef;

    protected final Disposable streamRequestSubscription;
  }

  @Value
  private static class BaseStreamContext {
    protected final StompConnectedFrame connectedFrame;

    protected final Flux<StompFrame> base;

    protected final FluxSinkRef<StompFrame> streamRequestSinkRef;
  }

  @Value
  private static class SharedStreamContext {
    protected final Flux<StompFrame> source;

    protected final FluxSinkRef<StompFrame> requestSinkRef;

    protected final FluxSink<Duration> heartbeatExpectationSink;
  }
}
