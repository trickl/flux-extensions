package com.trickl.flux.websocket.sockjs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.websocket.sockjs.frames.SockJsClose;
import com.trickl.flux.websocket.sockjs.frames.SockJsFrame;
import com.trickl.flux.websocket.sockjs.frames.SockJsHeartbeat;
import com.trickl.flux.websocket.sockjs.frames.SockJsMessage;
import com.trickl.flux.websocket.sockjs.frames.SockJsOpen;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import org.springframework.web.socket.sockjs.client.SockJsUrlInfo;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Log
@RequiredArgsConstructor
public class SockJsFluxClient {
  private final WebSocketClient webSocketClient;
  private final SockJsUrlInfo sockJsUrlInfo;
  private final Mono<HttpHeaders> webSocketHeadersProvider;
  private final ObjectMapper objectMapper;

  private static final String SOCK_JS_OPEN = "o";
  private static final String SOCK_JS_CLOSE = "c";
  private static final String SOCK_JS_HEARTBEAT = "h";

  /**
   * Get messages from the stream.
   *
   * @return A reactive stream of messages.
   */
  public Flux<SockJsFrame> get(Publisher<SockJsFrame> send) {
    EmitterProcessor<SockJsFrame> responseProcessor = EmitterProcessor.create();
    RawSockJsFluxClient sockJsClient =
        new RawSockJsFluxClient(
            webSocketClient,
            sockJsUrlInfo,
            webSocketHeadersProvider,
            objectMapper,
            () -> SOCK_JS_OPEN,
            () -> SOCK_JS_HEARTBEAT,
            (CloseStatus status) -> SOCK_JS_CLOSE + status.getCode());

    Publisher<SockJsFrame> sendWithResponse = Flux.merge(send, responseProcessor);

    return sockJsClient
        .get(Flux.from(sendWithResponse).flatMap(new ThrowableMapper<>(this::write)))
        .flatMap(new ThrowableMapper<String, SockJsFrame>(this::read))
        .onErrorContinue(JsonProcessingException.class, this::warnAndDropError)
        .doOnTerminate(() -> log.info("SockJsFluxClient client terminated"))
        .doOnCancel(() -> log.info("SockJsFluxClient client cancelled"))
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
    if (message.startsWith(SOCK_JS_OPEN)) {
      return new SockJsOpen();
    } else if (message.startsWith(SOCK_JS_HEARTBEAT)) {
      return new SockJsHeartbeat();
    } else if (message.startsWith(SOCK_JS_CLOSE)) {
      int closeStatus = Integer.parseInt(message.substring(SOCK_JS_CLOSE.length()));
      return new SockJsClose(closeStatus);
    }

    return new SockJsMessage(message);
  }

  protected String write(SockJsFrame request) throws JsonProcessingException {
    if (request.getClass().equals(SockJsMessage.class)) {
      return ((SockJsMessage) request).getPayload();
    }
    String errorMessage = MessageFormat.format("Sending {0} is not supported.", request.getClass());
    throw new UnsupportedOperationException(errorMessage);
  }
}
