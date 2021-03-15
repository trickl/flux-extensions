package com.trickl.flux.websocket;

import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import org.springframework.web.reactive.socket.WebSocketHandler;

public interface WebSocketHandlerFactory<T> {
  WebSocketHandler apply(Consumer<T> sink, Publisher<T> publisher);
}
