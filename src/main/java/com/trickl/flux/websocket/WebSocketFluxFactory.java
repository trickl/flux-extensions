package com.trickl.flux.websocket;

import reactor.core.publisher.Flux;

public interface WebSocketFluxFactory<T> {
  boolean canBuildFrom(WebSocketDestination request);
  
  Flux<T> build(WebSocketDestination request);
}