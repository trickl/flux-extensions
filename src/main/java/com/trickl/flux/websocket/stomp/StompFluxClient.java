package com.trickl.flux.websocket.stomp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.websocket.RobustWebSocketFluxClient;
import com.trickl.flux.websocket.stomp.frames.StompConnectFrame;
import com.trickl.flux.websocket.stomp.frames.StompConnectedFrame;
import com.trickl.flux.websocket.stomp.frames.StompDisconnectFrame;
import com.trickl.flux.websocket.stomp.frames.StompErrorFrame;
import com.trickl.flux.websocket.stomp.frames.StompHeartbeatFrame;
import com.trickl.flux.websocket.stomp.frames.StompMessageFrame;
import com.trickl.flux.websocket.stomp.frames.StompSubscribeFrame;
import com.trickl.flux.websocket.stomp.frames.StompUnsubscribeFrame;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.extern.java.Log;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@Log
public class StompFluxClient {
  private final RobustWebSocketFluxClient robustWebSocketFluxClient;

  private Duration connectionTimeout = Duration.ofSeconds(3);

  private Duration heartbeatSendFrequency = Duration.ofSeconds(5);

  private Duration heartbeatReceiveFrequency = Duration.ofSeconds(5);

  private ObjectMapper objectMapper = new ObjectMapper();

  @Builder
  StompFluxClient(
      WebSocketClient webSocketClient,
      Supplier<URI> transportUriProvider,
      ObjectMapper objectMapper,
      Supplier<HttpHeaders> webSocketHeadersProvider,
      Duration heartbeatSendFrequency,
      Duration heartbeatReceiveFrequency,
      Duration connectionTimeout,
      Duration disconnectionReceiptTimeout,
      Duration initialRetryDelay,
      Duration retryConsiderationPeriod,
      Mono<Void> doBeforeSessionOpen,
      Mono<Void> doAfterSessionClose,
      int maxRetries) {
    RobustWebSocketFluxClient.RobustWebSocketFluxClientBuilder robustWebSocketFluxClientBuilder =
        RobustWebSocketFluxClient.builder()
            .webSocketClient(webSocketClient)
            .transportUriProvider(transportUriProvider)
            .isConnectedFrame(this::isConnectedFrame)
            .isDataFrameForDestination(this::isDataFrameForDestination)
            .getHeartbeatSendFrequencyCallback(this::getHeartbeatSendFrequency)
            .getHeartbeatReceiveFrequencyCallback(this::getHeartbeatReceiveFrequency)
            .doConnect(this::doConnect)
            .buildDisconnectFrame(this::buildDisconnectFrame)
            .buildSubscribeFrame(this::buildSubscribeFrame)
            .buildUnsubscribeFrame(this::buildUnsubscribeFrame)
            .buildHeartbeatFrame(this::buildHeartbeatFrame)
            .buildErrorFrame(this::buildErrorFrame)
            .decodeErrorFrame(this::decodeErrorFrame);

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
    return ((StompConnectedFrame) connectedFrame).getHeartbeatReceiveFrequency();
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

  protected Optional<StompFrame> buildSubscribeFrame(String destination, String subscriptionId) {
    return  Optional.of(StompSubscribeFrame.builder()
    .destination(destination)
    .subscriptionId(subscriptionId)
    .build());
  }

  protected Optional<StompFrame> buildUnsubscribeFrame(String subscriptionId) {
    return Optional.of(StompUnsubscribeFrame.builder()
    .subscriptionId(subscriptionId)
    .build());
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
   * Subscribe to a destination.
   *
   * @param destination The destination channel
   * @param minMessageFrequency Unsubscribe if no message received in this time
   * @return A flux of messages on that channel
   */
  public <T> Flux<T> subscribe(
      String destination, Class<T> messageType, Duration minMessageFrequency) {
    return robustWebSocketFluxClient.subscribe(destination, minMessageFrequency)    
        .flatMap(new ThrowableMapper<>(frame -> decodeDataFrame(frame, messageType)));
  }
}
