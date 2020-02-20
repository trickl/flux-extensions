package com.trickl.flux.websocket.stomp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Log
@RequiredArgsConstructor
public class StompFluxClient {
  private final WebSocketClient webSocketClient;
  private final URI transportUri;
  private final Mono<HttpHeaders> webSocketHeadersProvider;
  private final ObjectMapper objectMapper;
  private final Duration heartbeatSendFrequency;
  private final Duration heartbeatReceiveFrequency;

  private final EmitterProcessor<StompFrame> responseProcessor = EmitterProcessor.create();

  private final EmitterProcessor<StompFrame> streamRequestProcessor = EmitterProcessor.create();

  private final FluxSink<StompFrame> streamRequestSink = streamRequestProcessor.sink();

  private final EmitterProcessor<Duration> heartbeatSendProcessor = EmitterProcessor.create();

  private final FluxSink<Duration> heartbeatSendSink = heartbeatSendProcessor.sink();

  private final EmitterProcessor<Duration> heartbeatExpectationProcessor =
      EmitterProcessor.create();

  private final FluxSink<Duration> heartbeatExpectationSink = heartbeatExpectationProcessor.sink();

  private final AtomicInteger maxSubscriptionNumber = new AtomicInteger(0);

  private final Map<String, String> subscriptionDestinationIdMap = new HashMap<>();

  private final AtomicReference<Flux<StompMessageFrame>> sharedStream = new AtomicReference<>();

  private final AtomicBoolean isConnected = new AtomicBoolean(false);

  private final AtomicBoolean isConnecting = new AtomicBoolean(false);

  @Setter private long maxRetriesOnError = 12;

  @Setter private Duration retryOnErrorFirstBackoff = Duration.ofSeconds(1);

  /** Connect to the stomp transport. */
  public void connect() {
    if (isConnected.get() || !isConnecting.compareAndSet(false, true)) {
      // Already connected
      return;
    }

    try {
      RawStompFluxClient stompFluxClient =
          new RawStompFluxClient(
              webSocketClient,
              transportUri,
              webSocketHeadersProvider,
              heartbeatSendFrequency,
              heartbeatReceiveFrequency);

      Flux<StompFrame> heartbeats = heartbeatSendProcessor.switchMap(this::createHeartbeats);
      Flux<StompMessageFrame> heartbeatExpectation =
          heartbeatExpectationProcessor.switchMap(this::timeoutNoHeartbeat);

      Publisher<StompFrame> sendWithResponse =
          Flux.merge(streamRequestProcessor, heartbeats, responseProcessor);

      Flux<StompMessageFrame> stream =
          stompFluxClient
              .get(sendWithResponse)
              .doOnNext(
                  frame -> {
                    log.info("Got frame " + frame.getClass());
                    if (StompConnectedFrame.class.equals(frame.getClass())) {
                      handleConnectStream((StompConnectedFrame) frame);
                    }
                  })
              .flatMap(new ThrowableMapper<StompFrame, StompFrame>(this::handleErrorFrame))
              .mergeWith(heartbeatExpectation)
              .filter(frame -> frame.getHeaderAccessor().getCommand().equals(StompCommand.MESSAGE))
              .cast(StompMessageFrame.class)
              .onErrorContinue(JsonProcessingException.class, this::warnAndDropError)
              .doOnError(this::sendErrorFrame)
              .doAfterTerminate(this::handleTerminateStream)
              .retryBackoff(maxRetriesOnError, retryOnErrorFirstBackoff)
              .publishOn(Schedulers.parallel())
              .publish()
              .refCount();

      sharedStream.set(stream);
    } finally {
      isConnecting.set(false);
    }
  }

  protected void handleTerminateStream() {
    isConnected.set(false);
    sharedStream.set(null);
  }

  protected void handleConnectStream(StompConnectedFrame frame) {
    expectHeartbeats(frame.getHeartbeatSendFrequency());
    sendHeartbeats(frame.getHeartbeatReceiveFrequency());
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
    StompFrame frame = StompErrorFrame.builder().message(error.getLocalizedMessage()).build();
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

  protected Publisher<StompMessageFrame> timeoutNoHeartbeat(Duration frequency) {
    if (frequency.isZero() || sharedStream.get() == null) {
      return Flux.empty();
    }

    return sharedStream
        .get()
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
        .ignoreElements()
        .log("Timeout Processor");
  }

  protected void sendHeartbeats(Duration frequency) {
    log.info("Sending heartbeats every " + frequency.toString());
    heartbeatSendSink.next(frequency);
  }

  protected void expectHeartbeats(Duration frequency) {
    log.info("Expecting heartbeats every " + frequency.toString());
    heartbeatExpectationSink.next(frequency);
  }

  protected void warnAndDropError(Throwable ex, Object value) {
    log.log(
        Level.WARNING,
        MessageFormat.format(
            "Json processing error.\n Message: {0}\nValue: {1}\n",
            new Object[] {ex.getMessage(), value}));
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
        sharedStream
            .get()
            .filter(frame -> frame.getDestination().equals(destination))
            .timeout(minMessageFrequency)
            .onErrorMap(
                error -> {
                  if (error instanceof TimeoutException) {
                    return new NoDataException("No data within " + minMessageFrequency, error);
                  }
                  return error;
                })
            .doOnTerminate(() -> unsubscribe(destination));

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
}
