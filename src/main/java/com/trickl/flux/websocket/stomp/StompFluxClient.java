package com.trickl.flux.websocket.stomp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.websocket.stomp.StompFrame;
import com.trickl.flux.websocket.stomp.frames.StompConnectedFrame;
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

@Log
@RequiredArgsConstructor
public class StompFluxClient {
  private final WebSocketClient webSocketClient;
  private final URI transportUri;
  private final Mono<HttpHeaders> webSocketHeadersProvider;
  private final ObjectMapper objectMapper;

  private final EmitterProcessor<StompFrame> responseProcessor = EmitterProcessor.create();

  private final EmitterProcessor<StompFrame> streamRequestProcessor = EmitterProcessor.create();

  private final FluxSink<StompFrame> streamRequestSink = streamRequestProcessor.sink();

  private final AtomicInteger maxSubscriptionNumber = new AtomicInteger(0);

  private final Map<String, String> subscriptionDestinationIdMap = new HashMap<>();

  private final AtomicReference<Flux<StompMessageFrame>> sharedStream = new AtomicReference<>();

  private final AtomicBoolean isConnected = new AtomicBoolean(false);

  private final AtomicBoolean isConnecting = new AtomicBoolean(false);

  @Setter
  private long maxRetriesOnError = 12;

  @Setter
  private Duration retryOnErrorFirstBackoff = Duration.ofSeconds(1);

  /**
   * Connect to the stomp transport.
   */
  public void connect() {
    if (isConnected.get() || !isConnecting.compareAndSet(false, true)) {
      // Already connected
      return;
    }
    
    try {
      RawStompFluxClient stompFluxClient =
          new RawStompFluxClient(
              webSocketClient, transportUri, webSocketHeadersProvider);

      Publisher<StompFrame> sendWithResponse =
          Flux.merge(streamRequestProcessor, responseProcessor);

      Flux<StompMessageFrame> stream = stompFluxClient.get(sendWithResponse)        
          .doOnNext(frame -> {
            log.info("Got frame " + frame.getClass());
            if (StompConnectedFrame.class.equals(frame.getClass())) {                
              handleConnectStream();
            }
          })
          .filter(frame -> frame.getHeaderAccessor().getCommand().equals(StompCommand.MESSAGE))
          .cast(StompMessageFrame.class)
          .onErrorContinue(JsonProcessingException.class, this::warnAndDropError)        
          .doAfterTerminate(this::handleTerminateStream)
          .retryBackoff(maxRetriesOnError, retryOnErrorFirstBackoff)
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


  protected void handleConnectStream() {
    isConnected.set(true);
    resubscribeAll();
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
    StompFrame frame = StompSubscribeFrame.builder()
        .destination(destination)
        .subscriptionId(subscriptionId)
        .build();
    streamRequestSink.next(frame);
    return subscriptionId;
  }

  protected void resubscribeAll() {
    subscriptionDestinationIdMap.replaceAll(
        (dest, id) -> subscribeDestination(dest));
  }
 
  /**
   * Subscribe to a destination.
   * 
   * @param destination The destination channel   
   * @return A flux of messages on that channel
   */
  public <T> Flux<T> subscribe(String destination, Class<T> messageType) {
    connect();
    subscriptionDestinationIdMap.computeIfAbsent(destination, this::subscribeDestination);      
    
    Flux<StompMessageFrame> messageFrameFlux = sharedStream.get()
        .filter(frame -> frame.getDestination().equals(destination))
        .doOnTerminate(() -> unsubscribe(destination));

    return messageFrameFlux.flatMap(
        new ThrowableMapper<>(frame -> readStompMessageFrame(frame, messageType)));
  }

  protected void unsubscribe(String destination) {
    subscriptionDestinationIdMap.computeIfPresent(destination, (dest, subscriptionId) -> {
      StompFrame frame = StompUnsubscribeFrame.builder()
          .subscriptionId(subscriptionId)
          .build();
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
   * @param <O> The type of object to send
   * @param message The message
   * @param destination The destination
   * @throws JsonProcessingException If the message cannot be JSON encoded
   */
  public <O> void sendMessage(O message, String destination) throws JsonProcessingException {
    String body = objectMapper.writeValueAsString(message);
    StompFrame frame = StompSendFrame.builder()
        .destination(destination)
        .body(body).build();
    streamRequestSink.next(frame);
  }
}

