package com.trickl.flux.websocket;

import com.trickl.flux.mappers.ThrowableMapper;
import java.util.function.Consumer;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class TextWebSocketHandler implements WebSocketHandler {

  private final Consumer<String> receive;

  private final Publisher<String> send;

  @Override
  public Mono<Void> handle(WebSocketSession session) {
    Mono<Void> input = session.receive()
        .log("receive", Level.FINER)
        .flatMap(new ThrowableMapper<>(this::handleMessage)).then()
        .doFinally(signalType -> log.info("Input completed with signal - " + signalType.name()));

    Mono<Void> output =
        session.send(Flux.from(send)
          .log("send", Level.FINER)
          .map(message -> createMessage(session, message)))
          .doFinally(signalType -> 
              log.info("Output completed with signal - " 
              + signalType.name())).log("output", Level.FINER);

    return Mono.zip(input, output).then()
        .then(Mono.fromRunnable(() -> log.info("BinaryWebSocketHandler complete")));
  }

  protected WebSocketMessage createMessage(WebSocketSession session, String message) {
    log.log(Level.FINE, "\u001B[34mSENDING {0}\u001B[0m", new Object[] {message});
    return session.textMessage(message);
  }

  protected WebSocketMessage handleMessage(WebSocketMessage message) {
    message.retain();
    String payload = message.getPayloadAsText();

    log.log(Level.FINE, "\u001B[34mRECEIVED {0}\u001B[0m", new Object[] {payload});
    receive.accept(payload);
    return message;
  }
}
