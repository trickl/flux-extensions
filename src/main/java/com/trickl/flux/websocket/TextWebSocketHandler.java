package com.trickl.flux.websocket;

import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

import org.reactivestreams.Publisher;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class TextWebSocketHandler implements WebSocketHandler {

  private final FluxSink<String> receive;

  private final Publisher<String> send;

  @Override
  public Mono<Void> handle(WebSocketSession session) {
    Mono<Void> input = session.receive()
        .doOnNext(this::handleMessage).log("handle").then();

    Mono<Void> output =
        session.send(
            Flux.from(send)
                .map(message -> createMessage(session, message)));

    return Mono.zip(input, output)
        .doOnTerminate(receive::complete)
        .then(session.close());
  }

  protected WebSocketMessage createMessage(WebSocketSession session, String message) {
    log.log(
        Level.FINE, "\u001B[34mSENDING {0}\u001B[0m", new Object[] {message});
    return session.textMessage(message);
  }

  protected void handleMessage(WebSocketMessage message) {
    log.log(Level.FINE, "Received message type - {0}", new Object[] {message.getType()});       
    String payload = message.getPayloadAsText();
    log.log(
        Level.FINE, "\u001B[34mRECEIVED {0}\u001B[0m", new Object[] {payload});
    receive.next(payload);    
  }
}
