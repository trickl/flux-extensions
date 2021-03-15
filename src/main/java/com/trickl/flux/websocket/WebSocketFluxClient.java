package com.trickl.flux.websocket;

import java.net.URI;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@Log
@Builder
public class WebSocketFluxClient<T> {

  protected final WebSocketClient webSocketClient;

  protected final Supplier<URI> transportUriProvider;

  @Nonnull
  protected final BiFunction<Consumer<T>, Publisher<T>, WebSocketHandler> handlerFactory;

  @Builder.Default private Mono<HttpHeaders> webSocketHeadersProvider
      = Mono.fromSupplier(HttpHeaders::new);

  @Builder.Default private Mono<Void> doBeforeOpen = Mono.empty();

  @Builder.Default private Function<Consumer<T>, Mono<Void>> doAfterOpen
      = response -> Mono.empty();

  @Builder.Default private Function<Publisher<T>, Mono<Void>> doBeforeClose 
      = response -> Mono.empty();

  @Builder.Default private Mono<Void> doAfterClose = Mono.empty();

  /**
   * Get a flux of messages from the stream.
   *
   * @param send flux of messages to send upstream
   * @return A flux of (untyped) objects
   */
  public Flux<T> get(Publisher<T> send) {
    Sinks.One<WebSocketSession> sessionSink = Sinks.one();
    Sinks.Many<T> receiveSink = Sinks.many().multicast().onBackpressureBuffer();
    Sinks.Many<T> sendAfterOpenSink = Sinks.many().multicast().onBackpressureBuffer();
    Sinks.Many<T> sendBeforeCloseSink = Sinks.many().multicast().onBackpressureBuffer();
    
    Mono<Void> openSocket = webSocketHeadersProvider
        .<Void>flatMap(
            headers -> {
              WebSocketHandler dataHandler = handlerFactory.apply(receiveSink::tryEmitNext, 
                  Flux.merge(send, sendAfterOpenSink.asFlux(), sendBeforeCloseSink.asFlux()
                  .log("openProcessor", Level.FINE)));
              SessionHandler sessionHandler =
                  new SessionHandler(
                      dataHandler, sessionSink);
              CloseHandler<T> closeHander = new CloseHandler<T>(
                  sessionHandler,
                  receiveSink.asFlux(),
                  doBeforeClose,
                  doAfterClose);
              URI transportUri = transportUriProvider.get();
              log.info("Connecting to " + transportUri);
              return webSocketClient
                  .execute(transportUri, headers, closeHander)
                  .log("WebSocketClient", Level.FINE);
            })
        .doFinally(signal -> {          
          receiveSink.tryEmitComplete();
          sendAfterOpenSink.tryEmitComplete();
          sendBeforeCloseSink.tryEmitComplete();
          log.info("Socket closed.");
        })
        .log("Connection", Level.FINER);

    return doBeforeOpen.thenMany(Flux.merge(
      openSocket.cast(WebSocketSession.class),
      sessionSink.asMono().flatMap(webSocketSession ->
        doAfterOpen.apply(sendAfterOpenSink::tryEmitNext).then(Mono.just(webSocketSession)))
    )).flatMap(webSocketSession -> {
      return  receiveSink.asFlux();
    });
  }

  @Value
  private static class SessionContext<T> {
    protected final WebSocketSession webSocketSession;
    protected final Publisher<T> receivePublisher;
  }
}
