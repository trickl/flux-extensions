package com.trickl.flux.websocket;

import java.net.URI;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import lombok.Builder;
import lombok.Value;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@Log
@Builder
public class WebSocketFluxClient<T> {

  protected final WebSocketClient webSocketClient;

  protected final Supplier<URI> transportUriProvider;

  protected final BiFunction<FluxSink<T>, Publisher<T>, WebSocketHandler> handlerFactory;

  @Builder.Default private Mono<HttpHeaders> webSocketHeadersProvider
      = Mono.fromSupplier(HttpHeaders::new);

  @Builder.Default private Mono<Void> doBeforeOpen = Mono.empty();

  @Builder.Default private Function<FluxSink<T>, Mono<Void>> doAfterOpen
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
    return Flux.<T, SessionContext<T>>usingWhen(
        openSession(send).log("websocketsession", Level.FINER),
        context -> Flux.from(context.getReceivePublisher()).log("receivePublisher", Level.INFO),
        context -> {
          log.info("Disposing of connection");
          return doBeforeClose.apply(context.getReceivePublisher())
              .log("do before close", Level.FINER)
              .then(closeSession(context)).log("cleanup-session");
        }).log("websocketfluxclient", Level.INFO);
  }

  protected Mono<SessionContext<T>> openSession(Publisher<T> send) {  
    EmitterProcessor<WebSocketSession> sessionProcessor = EmitterProcessor.create();
    FluxSink<WebSocketSession> sessionSink = sessionProcessor.sink();
    EmitterProcessor<T> receiveProcessor = EmitterProcessor.create();
    FluxSink<T> receiveSink = receiveProcessor.sink();
    EmitterProcessor<T> openProcessor = EmitterProcessor.create();
    FluxSink<T> openSink = openProcessor.sink();
    
    Mono<Void> openSocket = webSocketHeadersProvider
        .<Void>flatMap(
            headers -> {
              WebSocketHandler dataHandler = handlerFactory.apply(
                  receiveSink, Flux.merge(send, openProcessor));
              SessionHandler sessionHandler =
                  new SessionHandler(
                      dataHandler, sessionSink);
              URI transportUri = transportUriProvider.get();
              log.info("Connecting to " + transportUri);
              return webSocketClient
                  .execute(transportUri, headers, sessionHandler)
                  .log("WebSocketClient", Level.FINER);
            })
        .doOnError(receiveSink::error)
        .doFinally(signal -> {
          sessionSink.complete();
          receiveSink.complete();
          log.info("Socket closed.");
        })
        .log("Connection", Level.FINER);

    return doBeforeOpen.then(Mono.<SessionContext<T>, Disposable>using(
        openSocket::subscribe,
        subscription -> sessionProcessor.flatMap(
          webSocketSession -> doAfterOpen.apply(openSink).then(Mono.just(
            new SessionContext<T>(webSocketSession, receiveProcessor, subscription)))).next(),
        subscription -> {
          log.info("Socket opened.");
        }));
  }

  protected Mono<Void> closeSession(SessionContext<T> context) {
    return context.getWebSocketSession().close().log("close", Level.FINER)
        .then(Mono.create(sink -> {
          context.getSubscription().dispose();
          sink.success();             
        }))
        .then(doAfterClose.log("after-session-close", Level.FINER)
        ).log("closeSession", Level.FINER);
  }

  @Value
  private static class SessionContext<T> {
    protected final WebSocketSession webSocketSession;
    protected final Publisher<T> receivePublisher;
    protected final Disposable subscription;
  }
}
