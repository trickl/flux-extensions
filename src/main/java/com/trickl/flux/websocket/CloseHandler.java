package com.trickl.flux.websocket;

import com.trickl.flux.mappers.DelayCancel;
import java.time.Duration;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class CloseHandler<T> implements WebSocketHandler {

  private final WebSocketHandler impl;

  private final Publisher<T> receive;

  private final Function<Publisher<T>, Mono<Void>> doBeforeClose;

  private final Mono<Void> doAfterClose;

  private boolean closed  = false;

  @Override
  public Mono<Void> handle(WebSocketSession session) {    
    return Mono.from(DelayCancel.apply(impl.handle(session)
        .materialize()
        .flatMap(signal -> {
          CloseStatus status = signal.isOnError() 
              ? CloseStatus.SERVER_ERROR 
              : CloseStatus.NORMAL;         
          return Mono.defer(() -> close(session, status)).then(Mono.just(signal));         
        })
        .dematerialize(), Mono.defer(() -> close(session, CloseStatus.NORMAL))))    
        .log("CloseHandler")
        .then();
  }

  Mono<Void> close(WebSocketSession session, CloseStatus status) {
    if (closed) {
      return Mono.empty();
    } else {
      closed = true;
      if (!session.isOpen()) {
        // Already closed - usually in response to a request to close from
        // the other end of the connection.
        return doAfterClose;
      }
      return doBeforeClose.apply(receive)          
          .then(Mono.defer(() -> session.close(status)))
          .then(Mono.delay(Duration.ofSeconds(1)))
          .then(doAfterClose);
    }
  }
}
