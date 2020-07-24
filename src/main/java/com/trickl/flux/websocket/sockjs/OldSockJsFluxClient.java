package com.trickl.flux.websocket.sockjs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.retry.ExponentialBackoffRetry;
import com.trickl.flux.websocket.sockjs.frames.SockJsCloseFrame;
import com.trickl.flux.websocket.sockjs.frames.SockJsHeartbeatFrame;
import com.trickl.flux.websocket.sockjs.frames.SockJsMessageFrame;
import com.trickl.flux.websocket.sockjs.frames.SockJsOpenFrame;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.logging.Level;
import lombok.Builder;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import org.springframework.web.socket.sockjs.client.SockJsUrlInfo;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Log
@Builder
public class OldSockJsFluxClient {
  private final WebSocketClient webSocketClient;
  private final SockJsUrlInfo sockJsUrlInfo;
  private final Mono<HttpHeaders> webSocketHeadersProvider;
  private final ObjectMapper objectMapper;

  @Builder.Default private Runnable onTerminate = () -> { /* NOOP */ };

  @Builder.Default private  Duration initialRetryDelay = Duration.ofSeconds(1);
  
  @Builder.Default private  Duration retryConsiderationPeriod = Duration.ofSeconds(255);
  
  @Builder.Default private  int maxRetries = 8;

  /**
   * Get messages from the stream.
   *
   * @return A reactive stream of messages.
   */
  public Flux<SockJsFrame> get(Publisher<SockJsFrame> send) {
    RawSockJsFluxClient sockJsClient =
        RawSockJsFluxClient.builder()
            .webSocketClient(webSocketClient)
            .sockJsUrlInfo(sockJsUrlInfo)
            .webSocketHeadersProvider(webSocketHeadersProvider)
            .objectMapper(objectMapper)
            .build();

    return sockJsClient
        .get(Flux.from(send).flatMap(new ThrowableMapper<>(this::write)))
        .flatMap(new ThrowableMapper<String, SockJsFrame>(this::read))
        .flatMap(new SockJsAbnormalCloseProcessor())
        .onErrorContinue(JsonProcessingException.class, this::warnAndDropError)
        .doOnTerminate(onTerminate)
        .retryWhen(
           ExponentialBackoffRetry.builder() 
             .initialRetryDelay(initialRetryDelay)
             .considerationPeriod(retryConsiderationPeriod)
             .maxRetries(maxRetries)
             .name("sockJsClient")
             .build())
        .publishOn(Schedulers.parallel())
        .publish()
        .refCount();
  }

  protected void warnAndDropError(Throwable ex, Object value) {
    log.log(
        Level.WARNING,
        MessageFormat.format(
            "Json processing error.\n Message: {0}\nValue: {1}\n",
            new Object[] {ex.getMessage(), value}));
  }

  protected SockJsFrame read(String message) throws IOException {
    // Handle sockJs messages
    if (message.startsWith(RawSockJsFluxClient.SOCK_JS_OPEN)) {
      return SockJsOpenFrame.builder().build();
    } else if (message.startsWith(RawSockJsFluxClient.SOCK_JS_HEARTBEAT)) {
      return SockJsHeartbeatFrame.builder().build();
    } else if (message.startsWith(RawSockJsFluxClient.SOCK_JS_CLOSE)) {
      int code = Integer.parseInt(message.substring(
          RawSockJsFluxClient.SOCK_JS_CLOSE.length()));
      return SockJsCloseFrame.builder().closeStatus(new CloseStatus(code)).build();
    }

    return SockJsMessageFrame.builder().message(message).build();
  }

  protected String write(SockJsFrame request) throws JsonProcessingException {
    if (request.getClass().equals(SockJsMessageFrame.class)) {
      return ((SockJsMessageFrame) request).getMessage();
    }
    String errorMessage = MessageFormat.format("Sending {0} is not supported.", request.getClass());
    throw new UnsupportedOperationException(errorMessage);
  }
}
