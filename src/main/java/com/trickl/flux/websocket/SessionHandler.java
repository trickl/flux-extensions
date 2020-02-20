package com.trickl.flux.websocket;

import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class SessionHandler implements WebSocketHandler {

  private final WebSocketHandler impl;

  private final Consumer<String> onCreateSession;

  @Override
  public Mono<Void> handle(WebSocketSession session) {
    onCreateSession.accept(session.getId());
    return impl.handle(session);
  }
}
