package com.trickl.flux.websocket.sockjs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.exceptions.AbnormalTerminationException;
import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.routing.TopicSubscription;
import com.trickl.flux.websocket.RobustWebSocketFluxClient;
import com.trickl.flux.websocket.TextWebSocketHandler;
import com.trickl.flux.websocket.sockjs.frames.SockJsCloseFrame;
import com.trickl.flux.websocket.sockjs.frames.SockJsHeartbeatFrame;
import com.trickl.flux.websocket.sockjs.frames.SockJsMessageFrame;
import com.trickl.flux.websocket.sockjs.frames.SockJsOpenFrame;
import java.io.IOException;
import java.net.URI;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@Log
public class SockJsFluxClient<TopicT> {
  private final RobustWebSocketFluxClient<String, SockJsFrame, TopicT> robustWebSocketFluxClient;

  private Duration connectionTimeout = Duration.ofSeconds(3);

  private Duration heartbeatSendFrequency = Duration.ofSeconds(5);

  private Duration heartbeatReceiveFrequency = Duration.ofSeconds(5);

  private ObjectMapper objectMapper = new ObjectMapper();

  @Builder
  SockJsFluxClient(
      WebSocketClient webSocketClient,
      Supplier<URI> transportUriProvider,
      ObjectMapper objectMapper,
      Mono<HttpHeaders> webSocketHeadersProvider,
      BiPredicate<SockJsFrame, TopicT> isDataFrameForDestination,
      BiFunction<Set<TopicSubscription<TopicT>>, Set<TopicSubscription<TopicT>>, List<SockJsFrame>> 
          buildSubscribeFrames,
      BiFunction<Set<TopicSubscription<TopicT>>, Set<TopicSubscription<TopicT>>, List<SockJsFrame>> 
          buildUnsubscribeFrames,
      Duration heartbeatSendFrequency,
      Duration heartbeatReceiveFrequency,
      Duration connectionTimeout,
      Duration disconnectionReceiptTimeout,
      Duration initialRetryDelay,
      Duration retryConsiderationPeriod,
      Mono<Void> doBeforeSessionOpen,
      Mono<Void> doAfterSessionClose,
      int maxRetries) {
    RobustWebSocketFluxClient.RobustWebSocketFluxClientBuilder<String, SockJsFrame, TopicT>
        robustWebSocketFluxClientBuilder =
        RobustWebSocketFluxClient.<String, SockJsFrame, TopicT>builder()
            .webSocketClient(webSocketClient)
            .transportUriProvider(transportUriProvider)
            .handlerFactory(TextWebSocketHandler::new)
            .isConnectedFrame(this::isConnectedFrame)
            .getHeartbeatSendFrequencyCallback(this::getHeartbeatSendFrequency)
            .getHeartbeatReceiveFrequencyCallback(this::getHeartbeatReceiveFrequency)
            .doConnect(this::doConnect)
            .buildDisconnectFrame(this::buildDisconnectFrame)
            .buildHeartbeatFrame(this::buildHeartbeatFrame)
            .decodeErrorFrame(this::decodeErrorFrame)
            .encoder(new SockJsFrameEncoder(objectMapper))
            .decoder(new SockJsFrameDecoder(objectMapper));

    if (isDataFrameForDestination != null) {
      robustWebSocketFluxClientBuilder.isDataFrameForDestination(isDataFrameForDestination);
    }

    if (buildSubscribeFrames != null) {
      robustWebSocketFluxClientBuilder.buildSubscribeFrames(buildSubscribeFrames);
    }

    if (buildUnsubscribeFrames != null) {
      robustWebSocketFluxClientBuilder.buildUnsubscribeFrames(buildUnsubscribeFrames);
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
    this.objectMapper = Optional.ofNullable(objectMapper).orElse(this.objectMapper);
    this.heartbeatSendFrequency =
        Optional.ofNullable(heartbeatSendFrequency).orElse(this.heartbeatSendFrequency);
    this.heartbeatReceiveFrequency =
        Optional.ofNullable(heartbeatReceiveFrequency).orElse(this.heartbeatReceiveFrequency);
  }

  protected boolean isConnectedFrame(SockJsFrame frame) {
    return SockJsOpenFrame.class.equals(frame.getClass());
  }

  protected boolean isReceiptFrame(SockJsFrame frame) {
    return false;
  }

  protected boolean isDataFrameForDestination(SockJsFrame frame, TopicT destination) {
    if (!(frame instanceof SockJsMessageFrame)) {
      return false;
    }

    return true;
  }

  protected Duration getHeartbeatSendFrequency(SockJsFrame connectedFrame) {
    return heartbeatSendFrequency;
  }

  protected Duration getHeartbeatReceiveFrequency(SockJsFrame connectedFrame) {
    return heartbeatReceiveFrequency;
  }

  protected <T> T decodeDataFrame(SockJsFrame frame, Class<T> messageType)
      throws IOException {
    return objectMapper.readValue(((SockJsMessageFrame) frame).getMessage(), messageType);
  }

  protected Optional<Throwable> decodeErrorFrame(SockJsFrame frame) {
    if (frame instanceof SockJsCloseFrame) {
      CloseStatus closeStatus = ((SockJsCloseFrame) frame).getCloseStatus();
      if (!CloseStatus.NORMAL.equals(closeStatus)) {
        String errorMessage = MessageFormat.format(
            "Unexpected SockJS Close ({0}) - {1}", 
            closeStatus.getCode(), closeStatus.getReason());
        log.warning(errorMessage);
        
        return Optional.of(new AbnormalTerminationException(errorMessage));
      }
    }
    return Optional.empty();                
  }

  protected Optional<SockJsFrame> buildDisconnectFrame() {
    return Optional.of(SockJsCloseFrame.builder().closeStatus(CloseStatus.NORMAL).build());
  }

  protected Optional<SockJsFrame> buildHeartbeatFrame(Long count) {
    return Optional.of(SockJsHeartbeatFrame.builder().build());
  }

  protected Duration doConnect(FluxSink<SockJsFrame> streamRequestSink) {
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
        TopicT destination, 
        Class<T> messageType, 
        Duration minMessageFrequency, 
        Publisher<T> send) {
    Publisher<SockJsFrame> sendFrames = Flux.from(send).flatMap(
        new ThrowableMapper<>(
            message -> {
              return SockJsMessageFrame.builder()
              .message(objectMapper.writeValueAsString(message))
              .build();
            }));
    return robustWebSocketFluxClient.get(destination, minMessageFrequency, sendFrames)      
        .flatMap(new ThrowableMapper<>(frame -> decodeDataFrame(frame, messageType)));
  }
}
