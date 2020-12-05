package com.trickl.flux.websocket.stomp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.routing.TopicSubscription;
import com.trickl.flux.websocket.BinaryWebSocketHandler;
import com.trickl.flux.websocket.RobustWebSocketFluxClient;
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
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@Log
public class StompFluxClient {
  private final RobustWebSocketFluxClient<byte[], StompFrame, String> robustWebSocketFluxClient;

  private Duration connectionTimeout = Duration.ofSeconds(3);

  private Duration heartbeatSendFrequency = Duration.ofSeconds(5);

  private Duration heartbeatReceiveFrequency = Duration.ofSeconds(5);

  private ObjectMapper objectMapper = new ObjectMapper();

  private static final double HEARTBEAT_RECEIVE_TOLERANCE = 1.5;

  @Builder
  StompFluxClient(
      WebSocketClient webSocketClient,
      Supplier<URI> transportUriProvider,
      ObjectMapper objectMapper,
      Mono<HttpHeaders> webSocketHeadersProvider,
      Duration heartbeatSendFrequency,
      Duration heartbeatReceiveFrequency,
      Duration connectionTimeout,
      Duration disconnectionReceiptTimeout,
      Duration initialRetryDelay,
      Duration retryConsiderationPeriod,
      Mono<Void> doBeforeSessionOpen,
      Mono<Void> doAfterSessionClose,
      int maxRetries) {
    RobustWebSocketFluxClient.RobustWebSocketFluxClientBuilder<byte[], StompFrame, String>
        robustWebSocketFluxClientBuilder =
        RobustWebSocketFluxClient.<byte[], StompFrame, String>builder()
            .webSocketClient(webSocketClient)
            .transportUriProvider(transportUriProvider)
            .handlerFactory(BinaryWebSocketHandler::new)
            .isConnectedFrame(this::isConnectedFrame)
            .isDataFrameForDestination(this::isDataFrameForDestination)
            .getHeartbeatSendFrequencyCallback(this::getHeartbeatSendFrequency)
            .getHeartbeatReceiveFrequencyCallback(this::getHeartbeatReceiveFrequency)
            .doConnect(this::doConnect)
            .buildDisconnectFrame(this::buildDisconnectFrame)
            .buildSubscribeFrames(this::buildSubscribeFrames)
            .buildUnsubscribeFrames(this::buildUnsubscribeFrames)
            .buildHeartbeatFrame(this::buildHeartbeatFrame)
            .buildErrorFrame(this::buildErrorFrame)
            .decodeErrorFrame(this::decodeErrorFrame)
            .encoder(new StompFrameEncoder())
            .decoder(new StompFrameDecoder());

    if (webSocketHeadersProvider != null) {
      robustWebSocketFluxClientBuilder.webSocketHeadersProvider(webSocketHeadersProvider);
    }
    if (disconnectionReceiptTimeout != null) {
      robustWebSocketFluxClientBuilder.disconnectionReceiptTimeout(disconnectionReceiptTimeout);
    }
    if (initialRetryDelay != null) {
      robustWebSocketFluxClientBuilder.initialRetryDelay(initialRetryDelay);
    }
    if (retryConsiderationPeriod != null) {
      robustWebSocketFluxClientBuilder.retryConsiderationPeriod(retryConsiderationPeriod);
    }
    if (doBeforeSessionOpen != null) {
      robustWebSocketFluxClientBuilder.doBeforeSessionOpen(doBeforeSessionOpen);
    }
    if (doAfterSessionClose != null) {
      robustWebSocketFluxClientBuilder.doAfterSessionClose(doAfterSessionClose);
    }
    if (maxRetries != 0) {
      robustWebSocketFluxClientBuilder.maxRetries(maxRetries);
    }

    robustWebSocketFluxClient = robustWebSocketFluxClientBuilder.build();
    this.connectionTimeout = Optional.ofNullable(connectionTimeout).orElse(this.connectionTimeout);
    this.objectMapper = Optional.ofNullable(objectMapper).orElse(this.objectMapper);
    this.heartbeatSendFrequency =
        Optional.ofNullable(heartbeatSendFrequency).orElse(this.heartbeatSendFrequency);
    this.heartbeatReceiveFrequency =
        Optional.ofNullable(heartbeatReceiveFrequency).orElse(this.heartbeatReceiveFrequency);
  }

  protected boolean isConnectedFrame(StompFrame frame) {
    return StompConnectedFrame.class.equals(frame.getClass());
  }

  protected boolean isReceiptFrame(StompFrame frame) {
    return StompReceiptFrame.class.equals(frame.getClass());
  }

  protected boolean isDataFrameForDestination(StompFrame frame, String destination) {
    if (!(frame instanceof StompMessageFrame)) {
      return false;
    }

    return ((StompMessageFrame) frame).getDestination().equals(destination);
  }

  protected Duration getHeartbeatSendFrequency(StompFrame connectedFrame) {
    return ((StompConnectedFrame) connectedFrame).getHeartbeatSendFrequency();
  }

  protected Duration getHeartbeatReceiveFrequency(StompFrame connectedFrame) {
    Duration expectedReceivedFrequency = 
        ((StompConnectedFrame) connectedFrame).getHeartbeatReceiveFrequency();
    return expectedReceivedFrequency
        .plus(expectedReceivedFrequency
          .multipliedBy((long) (HEARTBEAT_RECEIVE_TOLERANCE * 100))
          .dividedBy(100));
  }

  protected <T> T decodeDataFrame(StompFrame frame, Class<T> messageType)
      throws IOException {
    return objectMapper.readValue(((StompMessageFrame) frame).getBody(), messageType);
  }

  protected Optional<Throwable> decodeErrorFrame(StompFrame frame) {
    if (frame instanceof StompErrorFrame) {
      return Optional.of(new RuntimeException(((StompErrorFrame) frame).getMessage()));
    }
    return Optional.empty();                
  }

  protected Optional<StompFrame> buildDisconnectFrame() {
    return Optional.of(StompDisconnectFrame.builder().build());
  }

  protected List<StompFrame> buildSubscribeFrames(
      Set<TopicSubscription<String>> addedTopics,
      Set<TopicSubscription<String>> allTopics) {
    log.info("Building subscribe frames");
    return addedTopics.stream().map(topic -> StompSubscribeFrame.builder()
    .destination(topic.getTopic())
    .subscriptionId(String.valueOf(topic.getId()))
    .build()).collect(Collectors.toList());
  }

  protected List<StompFrame> buildUnsubscribeFrames(
      Set<TopicSubscription<String>> removedTopics,
      Set<TopicSubscription<String>> allTopics) {
    return removedTopics.stream().map(topic -> StompUnsubscribeFrame.builder()
    .subscriptionId(String.valueOf(topic.getId()))
    .build()).collect(Collectors.toList());
  }

  protected Optional<StompFrame> buildHeartbeatFrame(Long count) {
    return Optional.of(new StompHeartbeatFrame());
  }

  protected Optional<StompFrame> buildErrorFrame(Throwable error) {
    return Optional.of(
      StompErrorFrame.builder().message(error.toString()).build());
  }

  protected Duration doConnect(FluxSink<StompFrame> streamRequestSink) {
    log.fine("Sending Stomp Connection Frame");
    StompConnectFrame connectFrame =
        StompConnectFrame.builder()
            .acceptVersion("1.0,1.1,1.2")
            .heartbeatSendFrequency(heartbeatSendFrequency)
            .heartbeatReceiveFrequency(heartbeatReceiveFrequency)
            .host(robustWebSocketFluxClient.getTransportUriProvider().get().getHost())
            .build();
    streamRequestSink.next(connectFrame);

    return connectionTimeout;
  }

  /**
   * Get a flux for a destination.
   *
   * @param <T> The type of messages on the flux.
   * @param destination The destination channel
   * @param messageType The class of messages on the flux.
   * @param minMessageFrequency Unsubscribe if no message received in this time
   * @param send Messages to send upstream
   * @return A flux of messages on that channel
   */
  public <T> Flux<T> get(
      String destination,
      Class<T> messageType, 
      Duration minMessageFrequency,
      Publisher<StompFrame> send) {
    return robustWebSocketFluxClient.get(destination, minMessageFrequency, send)    
        .flatMap(new ThrowableMapper<>(frame -> decodeDataFrame(frame, messageType)));
  }
}
