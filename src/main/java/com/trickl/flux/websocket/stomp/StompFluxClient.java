package com.trickl.flux.websocket.stomp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.websocket.RobustWebSocketFluxClient;
import com.trickl.flux.websocket.stomp.frames.StompConnectFrame;
import com.trickl.flux.websocket.stomp.frames.StompConnectedFrame;
import com.trickl.flux.websocket.stomp.frames.StompDisconnectFrame;
import com.trickl.flux.websocket.stomp.frames.StompErrorFrame;
import com.trickl.flux.websocket.stomp.frames.StompSubscribeFrame;
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
            .getHeartbeatSendFrequencyCallback(
                frame -> ((StompConnectedFrame) frame).getHeartbeatSendFrequency())
            .getHeartbeatReceiveFrequencyCallback(
                frame -> ((StompConnectedFrame) frame).getHeartbeatReceiveFrequency())
            .doConnect(this::doConnect)
            .getDisconnectFrame(() -> Optional.of(StompDisconnectFrame.builder().build()))
            .getSubscriptionFrame((destination, subscriptionId) -> 
              Optional.of(StompSubscribeFrame.builder()
              .destination(destination)
              .subscriptionId(subscriptionId)
              .build()))
            .getErrorFrame(error -> Optional.of(
                StompErrorFrame.builder().message(error.toString()).build()))
            .readErrorFrame(frame -> {
              if (frame instanceof StompErrorFrame) {
                return Optional.of(new RuntimeException(((StompErrorFrame) frame).getMessage()));
              }
              return Optional.empty();            
            });
    if (objectMapper != null) {
      robustWebSocketFluxClientBuilder.objectMapper(objectMapper);
    }
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
    this.heartbeatSendFrequency =
        Optional.ofNullable(heartbeatSendFrequency).orElse(this.heartbeatSendFrequency);
    this.heartbeatReceiveFrequency =
        Optional.ofNullable(heartbeatReceiveFrequency).orElse(this.heartbeatReceiveFrequency);
  }

  protected boolean isConnectedFrame(StompFrame frame) {
    return StompConnectedFrame.class.equals(frame.getClass());
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
    return robustWebSocketFluxClient.subscribe(destination, messageType, minMessageFrequency);
  }
}
