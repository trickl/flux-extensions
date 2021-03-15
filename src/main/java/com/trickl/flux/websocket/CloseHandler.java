package com.trickl.flux.websocket;

import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class CloseHandler<T> implements WebSocketHandler {

  private final WebSocketHandler impl;

  private final Publisher<T> receive;

  private final Function<Publisher<T>, Mono<Void>> doBeforeClose;

  private final Mono<Void> doAfterClose;

  @Override
  public Mono<Void> handle(WebSocketSession session) {    
    return impl.handle(session)
        .materialize()
        .flatMap(signal -> {
          CloseStatus status = signal.isOnError() 
              ? CloseStatus.SERVER_ERROR 
              : CloseStatus.NORMAL;         
          return close(session, status).then(Mono.just(signal));          
        })
        .dematerialize()
        .doOnCancel(() -> {
          close(session, CloseStatus.NORMAL).subscribe();
        })  
        .log("CloseHandler")
        .then();
  }

  Mono<Void> close(WebSocketSession session, CloseStatus status) {    
    if (!session.isOpen()) {
      // Already closed - usually in response to a request to close from
      // the other end of the connection.
      return doAfterClose;
    }
    return doBeforeClose.apply(receive)
        .then(session.close(status))
        .then(doAfterClose);
  }
}
