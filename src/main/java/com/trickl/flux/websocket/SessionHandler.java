package com.trickl.flux.websocket;

import java.text.MessageFormat;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@Log
public class SessionHandler implements WebSocketHandler {

  private final WebSocketHandler impl;

  private final FluxSink<WebSocketSession> sessionSink;

  @Override
  public Mono<Void> handle(WebSocketSession session) {
    log.info(MessageFormat.format("Handled opened session ({0})", session.getId()));
    sessionSink.next(session);
    return impl.handle(session);
  }
}
