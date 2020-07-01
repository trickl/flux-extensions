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
import java.util.function.Consumer;
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
import reactor.core.publisher.UnicastProcessor;

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

  protected <T> Flux<T> createSwitchablePublisherAndSink(
      Publisher<?> beforeOpenSignal, FluxSinkRef<T> sinkRef, String name) {
    log.info("Calling switchable publisher and sink for " + name);
    Flux<Publisher<T>> mergedPublishers = Flux.from(beforeOpenSignal).map(signal -> {
      log.info("Creating switchable publisher and sink " + name);
      UnicastProcessor<T> nextEmitter =  UnicastProcessor.<T>create();
      sinkRef.setSink(nextEmitter.sink());
      return nextEmitter;
    });

    return Flux.switchOnNext(mergedPublishers);
  }

  protected ResponseContext createResponseContext() {
    log.info("Creating response context");
    EmitterProcessor<Long> beforeOpenSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> beforeOpenSignalSink = beforeOpenSignalEmitter.sink();

    FluxSinkRef<Duration> connectionExpectationSinkRef = new FluxSinkRef<>();
    FluxSinkRef<StompFrame> streamRequestSinkRef = new FluxSinkRef<>();

    Flux<Duration> connectionExpectationFlux
        = createSwitchablePublisherAndSink(
          beforeOpenSignalEmitter, connectionExpectationSinkRef, "connectionExpectation");
    Disposable connectionSubscription = connectionExpectationFlux.subscribe();

    Flux<StompFrame> streamRequestFlux
        = createSwitchablePublisherAndSink(
          beforeOpenSignalEmitter, streamRequestSinkRef, "streamRequest").share()
        .log("streamRequest");
    Disposable streamRequestSubscription = streamRequestFlux.subscribe();

    EmitterProcessor<Long> connectedSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> connectedSignalSink = connectedSignalEmitter.sink();

    FluxSinkRef<Duration> heartbeatExpectationSinkRef = new FluxSinkRef<>();
    FluxSinkRef<Duration> heartbeatSendSinkRef = new FluxSinkRef<>();

    Flux<Duration> heartbeatExpectationFlux
        = createSwitchablePublisherAndSink(
          connectedSignalEmitter, heartbeatExpectationSinkRef, "heartbeatExpectation")
          .log("heartbeatExpectation");
    Disposable heartbeatExpectationSubscription = heartbeatExpectationFlux.subscribe();

    Flux<Duration> heartbeatSendFlux
        = createSwitchablePublisherAndSink(
          connectedSignalEmitter, heartbeatSendSinkRef, "heartbeatSend").log("heartbeatSend");
    Disposable heartbeatSendSubscription = heartbeatSendFlux.subscribe();

    EmitterProcessor<Long> requireReceiptSignalEmitter = EmitterProcessor.create();
    FluxSink<Long> requireReceiptSignalSink = requireReceiptSignalEmitter.sink();

    FluxSinkRef<Duration> receiptExpectationSinkRef = new FluxSinkRef<>();
    Flux<Duration> receiptExpectationFlux
        = createSwitchablePublisherAndSink(
          requireReceiptSignalEmitter, receiptExpectationSinkRef, "receiptExpectation");
    Disposable receiptSubscription = receiptExpectationFlux.subscribe();

    return new ResponseContext(
      beforeOpenSignalSink,
      connectedSignalSink,
      requireReceiptSignalSink,
      connectionExpectationFlux, 
      connectionExpectationSinkRef,
      connectionSubscription,
      streamRequestFlux,
      streamRequestSinkRef,
      streamRequestSubscription,
      heartbeatExpectationFlux,
      heartbeatExpectationSinkRef,
      heartbeatExpectationSubscription,
      heartbeatSendFlux,
      heartbeatSendSinkRef,
      heartbeatSendSubscription,
      receiptExpectationFlux, 
      receiptExpectationSinkRef,
      receiptSubscription);
  }

  protected BaseStreamContext createBaseStreamContext(
      ResponseContext responseContext,
      Flux<StompFrame> source,
      StompConnectedFrame connectedFrame) {
    log.info("Creating base stream context");

    return new BaseStreamContext(
      responseContext,
      connectedFrame.getHeartbeatSendFrequency(),
      connectedFrame.getHeartbeatReceiveFrequency(),
      source
    );
  }

  protected SharedStreamContext createSharedStreamContext(BaseStreamContext context,
      Consumer<FluxSinkRef<StompFrame>>  doAfterSharedSubscribe) {
    log.info("Creating shared stream context");
    
    Flux<StompFrame> sharedStream = context.getStream()        
        .onErrorContinue(JsonProcessingException.class, this::warnAndDropError)
        .onErrorMap(new WarnOnErrorMapper())
        .doOnSubscribe(sub -> {
          resubscribeAll(context.getResponseContext().getStreamRequestSinkRef());

          doAfterSharedSubscribe.accept(context.getResponseContext().getStreamRequestSinkRef());
        })
        .doOnSubscribe(sub -> {
          if (!context.getHeartbeatReceiveFrequency().isZero()) {
            Duration heartbeatExpectation = 
                context.getHeartbeatReceiveFrequency().plus(
                  context.getHeartbeatReceiveFrequency()
                    .multipliedBy((long) (HEARTBEAT_PERCENTAGE_TOLERANCE * 100))
                    .dividedBy(100));
            expectHeartbeatsEvery(
                context.getResponseContext().getHeartbeatExpectationSinkRef(),
                heartbeatExpectation);
          }

          sendHeartbeatsEvery(
              context.getResponseContext().getHeartbeatSendFrequencySinkRef(),
              context.getHeartbeatSendFrequency());
        })
        .doOnError(error -> sendErrorFrame(error,
           context.getResponseContext().getStreamRequestSinkRef()))
        .retryWhen(new ExponentialBackoffRetry(
          initialRetryDelay, retryConsiderationPeriod, maxRetries))
        .share()
        .log("sharedStream", Level.INFO);

    log.info("Returning SharedStreamContext.");
    return new SharedStreamContext(context, sharedStream);
  }

  protected Flux<SharedStreamContext> getSharedStreamContext(
      Consumer<FluxSinkRef<StompFrame>> doAfterSharedSubscribe) {
    return Flux.<SharedStreamContext, ResponseContext>usingWhen(
      Mono.fromSupplier(this::createResponseContext),
      context -> getSharedStreamContext(context, doAfterSharedSubscribe), 
      context -> {
        log.info("Cleaning up response context");   
        return Mono.create(sink -> {
          context.getConnectionExpectationSinkRef().getSink().complete();
          context.getStreamRequestSinkRef().getSink().complete();
          context.getConnectionExpectationSubscription().dispose();
          context.getStreamRequestSubscription().dispose();
          context.getHeartbeatExpectationSinkRef().getSink().complete();
          context.getHeartbeatSendFrequencySinkRef().getSink().complete();
          context.getHeartbeatExpectationSubscription().dispose();
          context.getHeartbeatSendFrequencySubscription().dispose();
          sink.success();
        });
      }
    )
    .retryWhen(new ExponentialBackoffRetry(
          initialRetryDelay, retryConsiderationPeriod, maxRetries))
    .log("baseStreamContext");    
  }

  protected Flux<SharedStreamContext> getSharedStreamContext(
      ResponseContext context, Consumer<FluxSinkRef<StompFrame>> doAfterSharedSubscribe) {
    log.info("Creating base stream context mono");
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
            .doBeforeOpen(doBeforeSessionOpen.then(Mono.fromRunnable(() -> 
              context.getBeforeOpenSignalSink().next(1L)
            )))
            .doAfterOpen(sink -> connect(
              sink,
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
        .cast(StompConnectedFrame.class)
        .log("connection", Level.INFO);

    Flux<StompFrame> heartbeats = Flux.from(
        context.getHeartbeatSendFrequencyProcessor())
        .<StompFrame>switchMap(
          frequency -> sendHeartbeats(frequency, 
          context.getStreamRequestSinkRef()))
        .log("heartbeats");
    Flux<StompFrame> heartbeatExpectation = Flux.defer(() -> createHeartbeatExpectation(
        context.getHeartbeatExpectationProcessor(), base));

    Flux<StompConnectedFrame> heartbeatExpectations = 
         heartbeatExpectation.mergeWith(heartbeats).cast(StompConnectedFrame.class);

    Flux<StompConnectedFrame> requireReceiptExpectation = Flux.defer(() -> createReceiptExpectation(
        context.getReceiptExpectationProcessor(), base)).cast(StompConnectedFrame.class);
        
    Flux<StompConnectedFrame> connectedWithExpectations = connected
        .mergeWith(Flux.defer(() -> createConnectionExpectation(
        context.getConnectionExpectationProcessor(), connected)
        ))
        .mergeWith(heartbeatExpectations)
        .mergeWith(requireReceiptExpectation);

    return connectedWithExpectations.flatMap(connectedFrame -> {
      return Flux.<SharedStreamContext, BaseStreamContext>usingWhen(
          Mono.fromSupplier(() -> 
              createBaseStreamContext(context, base, connectedFrame)),
        baseStreamContext -> Flux.<SharedStreamContext>create(sink -> {          
          sink.next(createSharedStreamContext(baseStreamContext, doAfterSharedSubscribe));
        })
        .doOnSubscribe(sub -> {
          baseStreamContext.getResponseContext().getConnectedSignalSink().next(1L);
        }).log("SharedStreamContext"),
        baseStreamContext -> {
          log.info("Cleaning up baseStreamContext");
          baseStreamContext.getResponseContext().getRequireReceiptSignalSink().next(1L);
          return disconnect(baseStreamContext.getStream(),          
          baseStreamContext.getResponseContext().getStreamRequestSinkRef(),
          baseStreamContext.getResponseContext().getReceiptExpectationSinkRef());  
        }
      )
      .log("baseStreamContext(2)", Level.INFO);
    });
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

  protected Mono<Void> connect(
      FluxSink<StompFrame> streamRequestSink,
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
      streamRequestSink.next(connectFrame);
      expectConnectionWithin(connectionExpectationSinkRef.getSink(), connectionTimeout);

      return Mono.empty();
    });
  }

  protected Mono<Void> disconnect(
      Publisher<StompFrame> response, 
      FluxSinkRef<StompFrame> streamRequestSinkRef,
      FluxSinkRef<Duration> receiptExpectationSinkRef) {
    disconnect(streamRequestSinkRef, receiptExpectationSinkRef);
    
    return Mono.from(Flux.from(response).filter(
      frame -> StompReceiptFrame.class.equals(frame.getClass()))     
    .cast(StompReceiptFrame.class).log("disconnect")).then();
  }

  protected void disconnect(
      FluxSinkRef<StompFrame> streamRequestSinkRef,
      FluxSinkRef<Duration> receiptExpectationSink) {
    StompDisconnectFrame disconnectFrame = StompDisconnectFrame.builder().build();
    log.info("Disconnecting...");
    receiptExpectationSink.getSink().next(disconnectionReceiptTimeout);
    streamRequestSinkRef.getSink().next(disconnectFrame);    
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
    log.info("Sending heartbeats every " + frequency.toString());
    if (frequency.isZero()) {
      return Flux.empty();
    }

    return Flux.interval(frequency)
        .map(count -> new StompHeartbeatFrame())
        .startWith(new StompHeartbeatFrame())
        .flatMap(heartbeat -> {
          streamRequestSinkRef.getSink().next(heartbeat);
          return Mono.<StompHeartbeatFrame>empty();
        }).log("heartbeats", Level.INFO);
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

  protected void sendHeartbeatsEvery(
      FluxSinkRef<Duration> heartbeatSendFrequencySink, Duration frequency) {
    log.info("Request sending heartbeats every " + frequency.toString());
    heartbeatSendFrequencySink.getSink().next(frequency);
  }

  protected void expectHeartbeatsEvery(
      FluxSinkRef<Duration> heartbeatExpectationSink, Duration frequency) {    
    log.info("Expecting heartbeats every " + frequency.toString());
    heartbeatExpectationSink.getSink().next(frequency);
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
    log.info("Subscribing to destination " + destination);
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
  public <T> Flux<T> subscribe(
      String destination, Class<T> messageType, Duration minMessageFrequency) {
    // Drain the subscription only once
    Consumer<FluxSinkRef<StompFrame>> doAfterSharedStreamSubscribe = sinkRef -> {
      log.info("Subscribing to " + destination);
      subscriptionDestinationIdMap.computeIfAbsent(destination, 
          d -> subscribeDestination(d, sinkRef));      
    };

    return getSharedStreamContext(doAfterSharedStreamSubscribe)
        .flatMap(sharedStreamContext -> 
      Flux.<T, SharedStreamContext>usingWhen(
          Mono.fromSupplier(() -> sharedStreamContext),
          context -> subscribe(
            context, destination, messageType, minMessageFrequency)
              .log("sharedStreamContext", Level.INFO),
          context -> Mono.empty())
    )    
    .onErrorMap(new WarnOnErrorMapper())
    .log("outerSubscribe", Level.INFO);
  }

  protected <T> Flux<T> subscribe(SharedStreamContext context, 
      String destination, Class<T> messageType, Duration minMessageFrequency) {    
    Flux<StompMessageFrame> messageFrameFlux =
        context.getSharedStream()
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
        .onErrorMap(new WarnOnErrorMapper())
        .doFinally(signal -> unsubscribe(destination, 
          context.getBaseStreamContext().getResponseContext().getStreamRequestSinkRef()))
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
    protected final FluxSink<Long> beforeOpenSignalSink;

    protected final FluxSink<Long> connectedSignalSink;

    protected final FluxSink<Long> requireReceiptSignalSink;
    
    protected final Publisher<Duration> connectionExpectationProcessor;

    protected final FluxSinkRef<Duration> connectionExpectationSinkRef;

    protected final Disposable connectionExpectationSubscription;
    
    protected final Publisher<StompFrame> streamRequestProcessor;

    protected final FluxSinkRef<StompFrame> streamRequestSinkRef;

    protected final Disposable streamRequestSubscription;

    protected final Publisher<Duration> heartbeatExpectationProcessor;

    protected final FluxSinkRef<Duration> heartbeatExpectationSinkRef;

    protected final Disposable heartbeatExpectationSubscription;
    
    protected final Publisher<Duration> heartbeatSendFrequencyProcessor;

    protected final FluxSinkRef<Duration> heartbeatSendFrequencySinkRef;

    protected final Disposable heartbeatSendFrequencySubscription;

    protected final Publisher<Duration> receiptExpectationProcessor;

    protected final FluxSinkRef<Duration> receiptExpectationSinkRef;

    protected final Disposable receiptExpectationSubscription;
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
