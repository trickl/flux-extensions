package com.trickl.flux.websocket;

import java.net.URI;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Log
@RequiredArgsConstructor
public class TextWebSocketFluxClient {

  private final WebSocketClient webSocketClient;

  private final URI transportUrl;

  private final Mono<HttpHeaders> webSocketHeadersProvider;

  /**
   * Get a flux of messages from the stream.
   *
   * @param send the flux of messages to send upstream
   * @return A flux of (untyped) objects
   */
  public Flux<String> get(Publisher<String> send) {
    EmitterProcessor<String> receiveProcessor = EmitterProcessor.create();
    return Flux.<String, Disposable>using(
        () -> connect(send, receiveProcessor.sink()).subscribeOn(Schedulers.parallel()).subscribe(),
        connection -> receiveProcessor,
        connection -> {
          connection.dispose();
          log.info("Connection disposed.");
        });
  }

  protected Mono<Void> connect(Publisher<String> send, FluxSink<String> receive) {
    TextWebSocketHandler handler = new TextWebSocketHandler(receive, Flux.from(send));

    return webSocketHeadersProvider
        .flatMap(headers -> webSocketClient.execute(transportUrl, headers, handler).log("client"))
        .doOnError(receive::error);
  }
}
