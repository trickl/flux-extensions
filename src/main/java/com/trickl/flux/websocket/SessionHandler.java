package com.trickl.flux.websocket;

import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class SessionHandler implements WebSocketHandler {

  private final WebSocketHandler impl;

  private final Consumer<String> onCreateSession;

  private final FluxSink<WebSocketSession> sessionSink;

  @Override
  public Mono<Void> handle(WebSocketSession session) {
    onCreateSession.accept(session.getId());
    sessionSink.next(session);
    return impl.handle(session);
  }
}
