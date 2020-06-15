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

  private static final double HEARTBEAT_PERCENTAGE_TOLERANCE = 2;

  protected Mono<Void> disconnectStream(SharedStreamContext context) {
    context.getHeartbeatExpectationSink().complete();
    return Mono.empty();
  }

  protected ResponseContext createResponseContext() {
    log.info("Creating response context");
    EmitterProcessor<Duration> connectionExpectationProcessor = EmitterProcessor.create();
    FluxSink<Duration> connectionExpectationSink = connectionExpectationProcessor.sink();
    
    EmitterProcessor<StompFrame> responseProcessor = EmitterProcessor.create();
    EmitterProcessor<StompFrame> streamRequestProcessor = EmitterProcessor.create();
    FluxSink<StompFrame> streamRequestSink = streamRequestProcessor.sink();

    return new ResponseContext(
      connectionExpectationProcessor, 
      connectionExpectationSink,
      responseProcessor,
      streamRequestProcessor.log("streamRequest"),
      streamRequestSink);
  }

  protected Flux<BaseStreamContext> getBaseStreamContext() {
    return Flux.<BaseStreamContext, ResponseContext>usingWhen(
      Mono.fromSupplier(this::createResponseContext),
      this::getBaseStreamContext,
      context -> {
        log.info("Cleaning up response context");
        context.getConnectionExpectationSink().complete();
        context.getStreamRequestSink().complete();
        return Mono.empty();
      }
    ).log("baseStreamContext");
  }

  protected Mono<BaseStreamContext> getBaseStreamContext(ResponseContext context) {
    log.info("Creating base stream context");
    Publisher<StompFrame> sendWithResponse = Flux.merge(
        Flux.from(context.getStreamRequestProcessor()), 
        Flux.from(context.getResponseProcessor()))
        .onErrorMap(new WarnOnErrorMapper());

    RawStompFluxClient stompFluxClient =
        RawStompFluxClient.builder()        
            .webSocketClient(webSocketClient)
            .transportUriProvider(transportUriProvider)
            .webSocketHeadersProvider(Mono.fromSupplier(webSocketHeadersProvider))
            .heartbeatSendFrequency(heartbeatSendFrequency)
            .heartbeatReceiveFrequency(heartbeatReceiveFrequency)
            .doBeforeSessionOpen(doBeforeSessionOpen)
            .doAfterOpen(connect(
              context.getStreamRequestSink(),
              context.getConnectionExpectationSink()))
            .doAfterSessionClose(doAfterSessionClose)
            .build();

    Flux<StompFrame> base = stompFluxClient        
          .get(sendWithResponse)
          .flatMap(new ThrowableMapper<StompFrame, StompFrame>(
              this::handleErrorFrame))
          .share()      
          .log("base", Level.FINE);
    
    Mono<StompConnectedFrame> connected = base.filter(
        frame -> StompConnectedFrame.class.equals(frame.getClass()))
        .mergeWith(Flux.defer(() -> createConnectionExpectation(
            context.getConnectionExpectationProcessor(), base)
            ))        
        .cast(StompConnectedFrame.class)        
        .log("connection", Level.FINE).next();

    return connected.map(connectedFrame -> 
      new BaseStreamContext(connectedFrame, base, context.getStreamRequestSink())
    );     
  }
  
  protected Flux<SharedStreamContext> connectStream() {

    return Flux.<SharedStreamContext, BaseStreamContext>usingWhen(
      getBaseStreamContext(),
      context -> Flux.generate(sink -> sink.next(connectStream(context))),
        context -> {
          log.info("Cleaning up connection");
          EmitterProcessor<Duration> receiptExpectationProcessor = EmitterProcessor.create();
          FluxSink<Duration> receiptExpectationSink = receiptExpectationProcessor.sink();
          disconnect(context.getStreamRequestSink(), receiptExpectationSink);
          return context.getBase().filter(
            frame -> StompReceiptFrame.class.equals(frame.getClass()))
            .mergeWith(Flux.defer(() -> createReceiptExpectation(
                receiptExpectationProcessor, context.getBase())))        
          .cast(StompReceiptFrame.class);
      }
    )
    .log("connectStream")
    .retryWhen(new ExponentialBackoffRetry(
      initialRetryDelay, retryConsiderationPeriod, maxRetries));
  }

  protected SharedStreamContext connectStream(BaseStreamContext context) {
    log.info("Creating shared stream context");
    EmitterProcessor<Duration> heartbeatSendProcessor = EmitterProcessor.create();
    Flux<StompFrame> heartbeats = heartbeatSendProcessor.switchMap(
        frequency -> sendHeartbeats(frequency, context.getStreamRequestSink()));
    FluxSink<Duration> heartbeatSendSink = heartbeatSendProcessor.sink();

    EmitterProcessor<Duration> heartbeatExpectationProcessor = EmitterProcessor.create();
    FluxSink<Duration> heartbeatExpectationSink = heartbeatExpectationProcessor.sink();

    handleStreamConnected(
        context.getConnectedFrame().getHeartbeatSendFrequency(),
        context.getConnectedFrame().getHeartbeatReceiveFrequency(),
        heartbeatExpectationSink,
        context.getStreamRequestSink(),
        heartbeatSendSink);

    Flux<StompFrame> sharedStream = context.getBase()          
        .mergeWith(Flux.defer(() -> createHeartbeatExpectation(
          heartbeatExpectationProcessor, context.getBase())))
        .mergeWith(heartbeats)   
        .onErrorContinue(JsonProcessingException.class, this::warnAndDropError)
        .onErrorMap(new WarnOnErrorMapper())
        .doOnError(error -> sendErrorFrame(error, context.getStreamRequestSink()))
        .share()
        .log("sharedStream", Level.INFO);

    return new SharedStreamContext(
      sharedStream, 
      context.getStreamRequestSink(),
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
      FluxSink<StompFrame> streamRequestSink,
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
    resubscribeAll(streamRequestSink);
  }


  protected Mono<Void> connect(
      FluxSink<StompFrame> streamRequestSink,
      FluxSink<Duration> connectionExpectationSink) {
    return Mono.defer(() -> {
      log.info("Sending connect frame after connection");
      StompConnectFrame connectFrame =
          StompConnectFrame.builder()
              .acceptVersion("1.0,1.1,1.2")
              .heartbeatSendFrequency(heartbeatSendFrequency)
              .heartbeatReceiveFrequency(heartbeatReceiveFrequency)
              .host(transportUriProvider.get().getHost())
              .build();
      streamRequestSink.next(connectFrame);
      connectionExpectationSink.next(connectionTimeout);

      return Mono.empty();
    });
  }

  protected void disconnect(
      FluxSink<StompFrame> streamRequestSink,
      FluxSink<Duration> receiptExpectationSink) {
    StompDisconnectFrame disconnectFrame = StompDisconnectFrame.builder().build();
    log.info("Disconnecting...");
    streamRequestSink.next(disconnectFrame);
    receiptExpectationSink.next(disconnectionReceiptTimeout);
    receiptExpectationSink.complete();
  }

  protected StompFrame handleErrorFrame(StompFrame frame) throws RemoteStreamException {
    if (StompErrorFrame.class.equals(frame.getClass())) {
      throw new RemoteStreamException(((StompErrorFrame) frame).getMessage());
    }
    return frame;
  }

  protected void sendErrorFrame(Throwable error, FluxSink<StompFrame> streamRequestSink) {
    StompFrame frame = StompErrorFrame.builder().message(error.toString()).build();
    streamRequestSink.next(frame);
  }

  protected Publisher<StompHeartbeatFrame> sendHeartbeats(
      Duration frequency, FluxSink<StompFrame> streamRequestSink) {
    if (frequency.isZero()) {
      return Flux.empty();
    }

    return Flux.interval(frequency)
        .map(count -> new StompHeartbeatFrame())
        .startWith(new StompHeartbeatFrame())
        .flatMap(heartbeat -> {
          streamRequestSink.next(heartbeat);
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
      String destination, FluxSink<StompFrame> streamRequestSink) {
    int subscriptionNumber = maxSubscriptionNumber.getAndIncrement();
    String subscriptionId = MessageFormat.format("sub-{0}", subscriptionNumber);
    StompFrame frame =
        StompSubscribeFrame.builder()
            .destination(destination)
            .subscriptionId(subscriptionId)
            .build();
    streamRequestSink.next(frame);
    return subscriptionId;
  }

  protected void resubscribeAll(FluxSink<StompFrame> streamRequestSink) {
    subscriptionDestinationIdMap.replaceAll(
        (dest, id) -> subscribeDestination(dest, streamRequestSink));
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
    return Flux.<T, SharedStreamContext>usingWhen(connectStream(),
      sharedStreamContext -> subscribe(
          sharedStreamContext, destination, messageType, minMessageFrequency),
      this::disconnectStream)
      .onErrorMap(new WarnOnErrorMapper())
      .log("outerSubscribe", Level.FINE);
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
              d -> subscribeDestination(d, context.getRequestSink()));
        })
        .onErrorMap(new WarnOnErrorMapper())
        .doFinally(signal -> unsubscribe(destination, context.getRequestSink()))
        .log("innerSubscribe");

    return messageFrameFlux.flatMap(
      new ThrowableMapper<>(frame -> readStompMessageFrame(frame, messageType)));
  }

  protected void unsubscribe(String destination, FluxSink<StompFrame> streamRequestSink) {
    subscriptionDestinationIdMap.computeIfPresent(
        destination,
        (dest, subscriptionId) -> {
          StompFrame frame = StompUnsubscribeFrame.builder().subscriptionId(subscriptionId).build();
          streamRequestSink.next(frame);
          return null;
        });
  }

  protected <T> T readStompMessageFrame(StompMessageFrame frame, Class<T> messageType)
      throws IOException {
    return objectMapper.readValue(frame.getBody(), messageType);
  }

  @Value
  private static class ResponseContext {
    protected final Publisher<Duration> connectionExpectationProcessor;

    protected final FluxSink<Duration> connectionExpectationSink;
    
    protected final Publisher<StompFrame> responseProcessor;

    protected final Publisher<StompFrame> streamRequestProcessor;

    protected final FluxSink<StompFrame> streamRequestSink;
  }

  @Value
  private static class BaseStreamContext {
    protected final StompConnectedFrame connectedFrame;

    protected final Flux<StompFrame> base;

    protected final FluxSink<StompFrame> streamRequestSink;
  }

  @Value
  private static class SharedStreamContext {
    protected final Flux<StompFrame> source;

    protected final FluxSink<StompFrame> requestSink;

    protected final FluxSink<Duration> heartbeatExpectationSink;
  }
}
