package com.trickl.flux.websocket;

import java.net.URI;

import lombok.RequiredArgsConstructor;

import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.client.WebSocketClient;

import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@RequiredArgsConstructor
public class BinaryWebSocketFluxClient {

  private final WebSocketClient webSocketClient;

  private final URI transportUrl;

  private final Mono<HttpHeaders> webSocketHeadersProvider;

  private final Runnable onConnect;

  private final Runnable onDisconnect;

  /**
   * Get a flux of messages from the stream.
   *
   * @param send flux of messages to send upstream
   * @return A flux of (untyped) objects
   */
  public Flux<byte[]> get(Publisher<byte[]> send) {
    EmitterProcessor<byte[]> connectionProcessor = EmitterProcessor.create();    
    return Flux.<byte[], Disposable>using(
        () -> connect(send, connectionProcessor.sink())
            .subscribeOn(Schedulers.parallel())
            .subscribe(),
        connection -> connectionProcessor,
        connection -> {
          onDisconnect.run();
          connection.dispose();          
        });
  }

  protected Mono<Void> connect(Publisher<byte[]> send, FluxSink<byte[]> receive) {
    BinaryWebSocketHandler dataHandler = new BinaryWebSocketHandler(receive, Flux.from(send));
    SessionHandler sessionHandler = new SessionHandler(dataHandler,
        sessionId -> onConnect.run());

    return webSocketHeadersProvider.flatMap(headers -> 
        webSocketClient.execute(transportUrl, headers, sessionHandler).log("client"))
        .doOnError(receive::error);
  }
}
