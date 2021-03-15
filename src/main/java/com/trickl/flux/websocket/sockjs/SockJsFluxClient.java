package com.trickl.flux.websocket.sockjs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.exceptions.AbnormalTerminationException;
import com.trickl.flux.mappers.FluxSinkAdapter;
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
import java.util.function.Consumer;
import java.util.function.Function;
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
import reactor.core.publisher.Sinks;

@Log
public class SockJsFluxClient<TopicT, T, S> {
  private final RobustWebSocketFluxClient<String, SockJsFrame, TopicT> robustWebSocketFluxClient;

  private Duration connectionTimeout = Duration.ofSeconds(3);

  private Duration heartbeatSendFrequency = Duration.ofSeconds(5);

  private Duration heartbeatReceiveFrequency = Duration.ofSeconds(5);

  private ObjectMapper objectMapper = new ObjectMapper();

  private final BiFunction<String, Class<T>, Publisher<T>> payloadDecoder;
  private final Function<S, Publisher<String>> payloadEncoder;
  private final ProtocolFrameHandler<T, S> handleProtocolFrames;
  private final Class<T> responseClazz;

  @Builder
  SockJsFluxClient(
      WebSocketClient webSocketClient,
      Supplier<URI> transportUriProvider,
      ObjectMapper objectMapper,
      Mono<HttpHeaders> webSocketHeadersProvider,
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
      ProtocolFrameHandler<T, S> handleProtocolFrames,
      Class<T> responseClazz,
      BiFunction<String, Class<T>, Publisher<T>> payloadDecoder,
      Function<S, Publisher<String>> payloadEncoder,
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
            .handleProtocolFrames(this::processProtocolFrames)
            .encoder(new SockJsFrameEncoder(objectMapper))
            .decoder(new SockJsFrameDecoder(objectMapper));

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
    this.payloadDecoder = Optional.ofNullable(payloadDecoder)
        .orElse((payload, clazz) -> decodeDataFrame(payload, clazz));

    this.payloadEncoder = Optional.ofNullable(payloadEncoder)
      .orElse((data) -> encodeDataFrame(data));

    this.handleProtocolFrames = Optional.ofNullable(handleProtocolFrames)
      .orElse((data, responseSink, onComplete) -> {});

    this.responseClazz = responseClazz;
  }

  protected Publisher<SockJsFrame> processProtocolFrames(
      SockJsFrame frame, FluxSink<SockJsFrame> responseSink, Runnable onComplete) {

    return Mono.just(frame)
      .flatMapMany(f -> {
        if (f.getClass().equals(SockJsMessageFrame.class)) {
          SockJsMessageFrame messageFrame = (SockJsMessageFrame) f;
          FluxSinkAdapter<S, SockJsFrame, IOException> sendSink
              = new FluxSinkAdapter<>(responseSink, (sendData) -> {
                return Flux.from(encodeDataFrame(sendData))
                  .map(message -> 
                   SockJsMessageFrame.builder()
                    .message(message)
                    .build()
                  );
              });

          Mono<SockJsFrame> protocolActions = Flux.from(
              Flux.from(payloadDecoder.apply(messageFrame.getMessage(), responseClazz))
              .doOnNext(data -> handleProtocolFrames.accept(data, sendSink, onComplete))
          ).ignoreElements().cast(SockJsFrame.class);
    
          return Flux.just(f).mergeWith(protocolActions);
        }

        return Mono.just(f);
      });
  }

  protected boolean isConnectedFrame(SockJsFrame frame) {
    return SockJsOpenFrame.class.equals(frame.getClass());
  }

  protected boolean isReceiptFrame(SockJsFrame frame) {
    return false;
  }

  protected Duration getHeartbeatSendFrequency(SockJsFrame connectedFrame) {
    return heartbeatSendFrequency;
  }

  protected Duration getHeartbeatReceiveFrequency(SockJsFrame connectedFrame) {
    return heartbeatReceiveFrequency;
  }

  @SuppressWarnings("unchecked")
  protected Publisher<T> decodeDataFrame(String payload, Class<T> messageType) {
    if (messageType == null) {
      throw new NullPointerException("messageType");
    }

    if (String.class.equals(messageType)) {
      return Mono.just((T) payload);
    }

    return Flux.just(payload)
            .flatMap(new ThrowableMapper<>(
                p -> objectMapper.readValue(p, messageType)));
  }

  protected Publisher<String> encodeDataFrame(S data) {
    return Flux.just(data)
            .flatMap(new ThrowableMapper<>(
                p -> objectMapper.writeValueAsString(p)));
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

  protected Duration doConnect(Consumer<SockJsFrame> streamRequestSink) {
    return connectionTimeout;
  }

  /**
   * Get a flux for a destination.
   *
   * @param destination The destination channel
   * @param messageType The class of messages on the flux.
   * @param minMessageFrequency Unsubscribe if no message received in this time
   * @param send Messages to send upstream
   * @return A flux of messages on that channel
   */
  public Flux<T> get(
        TopicT destination, 
        Class<T> messageType, 
        Duration minMessageFrequency, 
        Publisher<S> send) {
    Publisher<SockJsFrame> sendFrames = Flux.from(send)
        .flatMap(payloadEncoder)
        .map(message -> {
          return SockJsMessageFrame.builder()
          .message(message)
          .build();
        });
    return robustWebSocketFluxClient.get(destination, minMessageFrequency, sendFrames)
        .filter(frame -> frame.getClass().equals(SockJsMessageFrame.class))
        .cast(SockJsMessageFrame.class)
        .map(messageFrame -> messageFrame.getMessage())
        .flatMap(message -> payloadDecoder.apply(message, messageType));
  }

  @FunctionalInterface
  public interface ProtocolFrameHandler<T, S> {
    void accept(T message, FluxSink<S> send, Runnable onComplete);
  }
}
