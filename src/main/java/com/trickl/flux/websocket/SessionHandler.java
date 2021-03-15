package com.trickl.flux.websocket;

import java.text.MessageFormat;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@RequiredArgsConstructor
@Log
public class SessionHandler implements WebSocketHandler {

  private final WebSocketHandler impl;

  private final Sinks.One<WebSocketSession> sessionSink;

  @Override
  public Mono<Void> handle(WebSocketSession session) {
    log.info(MessageFormat.format("Handled opened session ({0})", session.getId()));
    sessionSink.tryEmitValue(session);
    return impl.handle(session);
  }
}
