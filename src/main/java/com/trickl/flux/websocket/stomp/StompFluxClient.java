package com.trickl.flux.websocket.stomp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.exceptions.ConnectionTimeoutException;
import com.trickl.exceptions.MissingHeartbeatException;
import com.trickl.exceptions.NoDataException;
import com.trickl.exceptions.RemoteStreamException;
import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.websocket.stomp.frames.StompConnectedFrame;
import com.trickl.flux.websocket.stomp.frames.StompErrorFrame;
import com.trickl.flux.websocket.stomp.frames.StompHeartbeatFrame;
import com.trickl.flux.websocket.stomp.frames.StompMessageFrame;
import com.trickl.flux.websocket.stomp.frames.StompSendFrame;
import com.trickl.flux.websocket.stomp.frames.StompSubscribeFrame;
import com.trickl.flux.websocket.stomp.frames.StompUnsubscribeFrame;
import java.io.IOException;
import java.net.URI;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
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

  @Builder.Default private Duration connectionTimeout = Duration.ofSeconds(10);

  @Builder.Default private Runnable beforeConnect = () -> { /* Noop */ };

  @Builder.Default private Runnable afterDisconnect = () -> { /* Noop */ };

  private FluxSink<StompFrame> streamRequestSink;

  private final EmitterProcessor<Duration> heartbeatSendProcessor = EmitterProcessor.create();

  private final FluxSink<Duration> heartbeatSendSink = heartbeatSendProcessor.sink();

  private final AtomicInteger maxSubscriptionNumber = new AtomicInteger(0);

  private final Map<String, String> subscriptionDestinationIdMap = new HashMap<>();

  private final EmitterProcessor<Flux<StompFrame>> streamEmitter = 
      EmitterProcessor.create();

  private final Publisher<Flux<StompFrame>> streamPublisher = 
      streamEmitter.cache(1);

  private final FluxSink<Flux<StompFrame>> streamSink = streamEmitter.sink();

  private final AtomicBoolean isConnected = new AtomicBoolean(false);

  private final AtomicBoolean isConnecting = new AtomicBoolean(false);

  private static final double HEARTBEAT_PERCENTAGE_TOLERANCE = 2;
  
  /** Connect to the stomp transport. */
  public void connect() {
    if (isConnected.get() || !isConnecting.compareAndSet(false, true)) {
      // Already connected
      return;
    }

    RawStompFluxClient stompFluxClient =
        RawStompFluxClient.builder()
            .webSocketClient(webSocketClient)
            .transportUriProvider(transportUriProvider)
            .webSocketHeadersProvider(webSocketHeadersProvider)
            .heartbeatSendFrequency(heartbeatSendFrequency)
            .heartbeatReceiveFrequency(heartbeatReceiveFrequency)
            .beforeConnect(beforeConnect)
            .afterDisconnect(afterDisconnect)
            .build();

    Publisher<StompFrame> sendWithResponse = buildSendWithResponse()
        .switchIfEmpty(Flux.defer(this::buildSendWithResponse))
        .log("Send with response", Level.FINER);

    Flux<StompFrame> stream =
        Flux.<StompFrame, ConnectionContext>using(
            () -> createConnectionContext(connectionTimeout),
          context -> stompFluxClient        
          .get(sendWithResponse)   
          .doOnNext(
              frame -> {
                if (StompConnectedFrame.class.equals(frame.getClass())) {
                  handleConnectStream(context, (StompConnectedFrame) frame);
                }
              })
          .flatMap(new ThrowableMapper<StompFrame, StompFrame>(this::handleErrorFrame))
          .mergeWith(Flux.defer(() -> createConnectionExpectation(context)))
          .mergeWith(Flux.defer(() -> createHeartbeatExpectation(context)))          
          .onErrorContinue(JsonProcessingException.class, this::warnAndDropError)
          .doOnError(this::sendErrorFrame)
          .doAfterTerminate(this::handleTerminateStream),        
          this::closeConnectionContext)
        .retryWhen(this::retryWhenFactory)
        .publish()
        .refCount();
        
    streamSink.next(stream);
  }

  protected ConnectionContext createConnectionContext(Duration connectionTimeout) {
    return new ConnectionContext(connectionTimeout);
  }
  
  protected void closeConnectionContext(ConnectionContext context) {
    context.close();
  }

  protected Publisher<StompFrame> createConnectionExpectation(
      ConnectionContext context) {
    log.info("Creating connection expectation.");
    return Flux.from(context.getConnectionExpectationPublisher())
        .switchMap(this::timeoutNoConnection);
  }

  protected Publisher<StompFrame> createHeartbeatExpectation(
      ConnectionContext context) {
    return Flux.from(context.getHeartbeatExpectationPublisher())
        .switchMap(this::timeoutNoHeartbeat);    
  }

  protected void handleTerminateStream() {
    isConnecting.set(false);
    isConnected.set(false);
  }

  protected void handleConnectStream(ConnectionContext context, StompConnectedFrame frame) {
    if (!frame.getHeartbeatSendFrequency().isZero()) {
      Duration heartbeatExpectation = 
          frame.getHeartbeatSendFrequency().plus(
              frame.getHeartbeatSendFrequency()
              .multipliedBy((long) (HEARTBEAT_PERCENTAGE_TOLERANCE * 100))
              .dividedBy(100));
      expectHeartbeatsEvery(context, heartbeatExpectation);
    }
    sendHeartbeatsEvery(frame.getHeartbeatReceiveFrequency());
    expectConnectionWithin(context, Duration.ZERO);
    isConnecting.set(false);
    isConnected.set(true);
    resubscribeAll();
  }

  protected StompFrame handleErrorFrame(StompFrame frame) throws RemoteStreamException {
    if (StompErrorFrame.class.equals(frame.getClass())) {
      throw new RemoteStreamException(((StompErrorFrame) frame).getMessage());
    }
    return frame;
  }

  protected void sendErrorFrame(Throwable error) {
    StompFrame frame = StompErrorFrame.builder().message(error.toString()).build();
    streamRequestSink.next(frame);
  }

  protected Publisher<StompHeartbeatFrame> createHeartbeats(Duration frequency) {
    if (frequency.isZero()) {
      return Flux.empty();
    }

    return Flux.interval(frequency)
        .map(count -> new StompHeartbeatFrame())
        .startWith(new StompHeartbeatFrame());
  }

  protected Flux<StompFrame> buildSendWithResponse() {
    Flux<StompFrame> heartbeats = heartbeatSendProcessor.switchMap(this::createHeartbeats);    

    EmitterProcessor<StompFrame> responseProcessor = EmitterProcessor.create();
    EmitterProcessor<StompFrame> streamRequestProcessor = EmitterProcessor.create();
    streamRequestSink = streamRequestProcessor.sink();
    return Flux.merge(streamRequestProcessor, heartbeats, responseProcessor);
  }

  protected Publisher<StompFrame> timeoutNoHeartbeat(Duration frequency) {
    return Flux.from(streamPublisher).switchMap(stream -> {
      if (frequency.isZero()) {
        log.info("Cancelling heartbeat expectation");
        return Flux.empty();
      }
      log.info("Expecting heartbeats every " + frequency.toString());

      return stream
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
    });
  }

  protected Publisher<StompFrame> timeoutNoConnection(Duration period) {    
    return Flux.from(streamPublisher).switchMap(stream -> {
      if (period.isZero()) {
        log.info("Cancelling timeout expectation " + period);
        return Flux.empty();
      }

      log.info("Expecting a connection within " + period.toString());
      return stream
        .timeout(period)
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
    });
  }

  protected void sendHeartbeatsEvery(Duration frequency) {
    log.info("Sending heartbeats every " + frequency.toString());
    heartbeatSendSink.next(frequency);
  }

  protected void expectHeartbeatsEvery(ConnectionContext context, Duration frequency) {    
    context.getHeartbeatExpectationSink().next(frequency);
  }

  protected void expectConnectionWithin(ConnectionContext context, Duration period) {    
    context.getConnectionExpectationSink().next(period);
  }

  protected void warnAndDropError(Throwable ex, Object value) {
    log.log(
        Level.WARNING,
        MessageFormat.format(
            "Json processing error.\n Message: {0}\nValue: {1}\n",
            new Object[] {ex.getMessage(), value}));
  }

  protected Flux<Long> retryWhenFactory(Flux<Throwable> errorFlux) {
    Duration initialRetryDelay = Duration.ofSeconds(1);
    int maxRetries = 3;

    return errorFlux
        .scan(
            Collections.<Throwable>emptyList(),
            (List<Throwable> last, Throwable latest) ->
                Stream.concat(last.stream(), Stream.of(latest)).collect(Collectors.toList()))
        .map(List::size)
        .flatMap(
            errorCount -> {
              if (errorCount > maxRetries) {
                return Mono.error(new IllegalStateException("Max retries exceeded"));
              } else if (errorCount > 0) {
                Duration retryDelay = getExponentialRetryDelay(initialRetryDelay, errorCount);
                log.info("Will retry after error in " + retryDelay);
                return Mono.delay(retryDelay).doOnNext(x -> log.info("Retrying..."));
              }
              return Mono.empty();
            });
  }

  protected Duration getExponentialRetryDelay(Duration initial, int errorCount) {
    long initialMs = initial.toMillis();
    long exponentialMs = initialMs * (long) Math.pow(2, errorCount - 1.0);
    return Duration.ofMillis(exponentialMs);
  }

  protected String subscribeDestination(String destination) {
    if (!isConnected.get()) {
      return "sub-disconnected";
    }

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

  protected void resubscribeAll() {
    subscriptionDestinationIdMap.replaceAll((dest, id) -> subscribeDestination(dest));
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
    connect();
    subscriptionDestinationIdMap.computeIfAbsent(destination, this::subscribeDestination);

    Flux<StompMessageFrame> messageFrameFlux =
        Flux.from(streamPublisher).switchMap(stream ->
            stream
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
            .doFinally(signal -> unsubscribe(destination)));

    return messageFrameFlux.flatMap(
        new ThrowableMapper<>(frame -> readStompMessageFrame(frame, messageType)));
  }

  protected void unsubscribe(String destination) {
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

  /**
   * Send a message to a destination.
   *
   * @param <O> The type of object to send
   * @param message The message
   * @param destination The destination
   * @throws JsonProcessingException If the message cannot be JSON encoded
   */
  public <O> void sendMessage(O message, String destination) throws JsonProcessingException {
    String body = objectMapper.writeValueAsString(message);
    StompFrame frame = StompSendFrame.builder().destination(destination).body(body).build();
    streamRequestSink.next(frame);
  }

  private static class ConnectionContext {
    private final EmitterProcessor<Duration> connectionExpectationProcessor =
        EmitterProcessor.create();
    private final FluxSink<Duration> connectionExpectationSink =
        connectionExpectationProcessor.sink();
    private final EmitterProcessor<Duration> heartbeatExpectationProcessor =
        EmitterProcessor.create();
    private final FluxSink<Duration> heartbeatExpectationSink = 
        heartbeatExpectationProcessor.sink();

    public ConnectionContext(Duration connectionTimeout) {
      log.info("Creating new connection context");
      connectionExpectationSink.next(connectionTimeout);
    }

    public Publisher<Duration> getConnectionExpectationPublisher() {
      return connectionExpectationProcessor;
    }

    public FluxSink<Duration> getConnectionExpectationSink() {
      return connectionExpectationSink;
    }

    public Publisher<Duration> getHeartbeatExpectationPublisher() {
      return heartbeatExpectationProcessor;
    }

    public FluxSink<Duration> getHeartbeatExpectationSink() {
      return heartbeatExpectationSink;
    }

    public void close() {
      log.info("Closing connection context");
      connectionExpectationSink.complete();
      heartbeatExpectationSink.complete();
    }
  }
}
