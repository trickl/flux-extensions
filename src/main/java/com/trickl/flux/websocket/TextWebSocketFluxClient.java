package com.trickl.flux.websocket;

import java.net.URI;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Log
@Builder
public class TextWebSocketFluxClient {

  private static final Runnable NOOP_RUNNABLE =
      () -> {
        /* Noop */
      };

  private static final Consumer<FluxSink<String>> NOOP_SINK_CONSUMER =
      (FluxSink<String> sink) -> {
        /* Noop */
      };

  protected final WebSocketClient webSocketClient;

  protected final Supplier<URI> transportUriProvider;

  @Builder.Default private Mono<HttpHeaders> webSocketHeadersProvider
      = Mono.fromSupplier(HttpHeaders::new);

  @Builder.Default private Duration sessionTimeout = Duration.ofSeconds(5);

  @Builder.Default private Runnable beforeConnect = NOOP_RUNNABLE;

  @Builder.Default private Consumer<FluxSink<String>> afterConnect = NOOP_SINK_CONSUMER;

  @Builder.Default private Consumer<FluxSink<String>> beforeDisconnect = NOOP_SINK_CONSUMER;

  @Builder.Default private Runnable afterDisconnect = NOOP_RUNNABLE;

  /**
   * Get a flux of messages from the stream.
   *
   * @param send flux of messages to send upstream
   * @return A flux of (untyped) objects
   */
  public Flux<String> get(Publisher<String> send) {
    return Flux.<String, SessionSubscriber>using(
        () -> {
          EmitterProcessor<WebSocketSession> sessionProcessor = EmitterProcessor.create();
          FluxSink<WebSocketSession> sessionSink = sessionProcessor.sink();
          EmitterProcessor<String> receiveProcessor = EmitterProcessor.create();
          FluxSink<String> receiveSink = receiveProcessor.sink();
          EmitterProcessor<String> sendProcessor = EmitterProcessor.create();
          FluxSink<String> sendSink = sendProcessor.sink();
          return subscribeConnection(
              Flux.merge(send, sendProcessor),
              sendSink,
              receiveProcessor,
              receiveSink,
              sessionProcessor,
              sessionSink);
        },
        // Can we merge this??
        SessionSubscriber::getReceivePublisher,
        subscriber -> {
          beforeDisconnect.accept(subscriber.getSendSink());
          subscriber.onComplete();
        })
        .doFinally(
            signal -> 
              afterDisconnect.run()
            );
  }

  protected SessionSubscriber subscribeConnection(
      Publisher<String> send,
      FluxSink<String> sendSink,
      Publisher<String> receivePublisher,
      FluxSink<String> receiveSink,
      Publisher<WebSocketSession> sessionPublisher,
      FluxSink<WebSocketSession> sessionSink) {
    return webSocketHeadersProvider
        .<Void>flatMap(
            headers -> {
              beforeConnect.run();
              TextWebSocketHandler dataHandler = new TextWebSocketHandler(receiveSink, send);
              SessionHandler sessionHandler =
                  new SessionHandler(
                      dataHandler, sessionId -> afterConnect.accept(sendSink), sessionSink);
              URI transportUri = transportUriProvider.get();
              log.info("Connecting to " + transportUri);
              return webSocketClient
                  .execute(transportUri, sessionHandler)
                  .log("WebSocketClient", Level.FINER);
            })
        .doOnError(receiveSink::error)
        .doFinally(
            signal -> {
              receiveSink.complete();
              sendSink.complete();
            })
        .log("Connection", Level.FINER)
        .subscribeOn(Schedulers.parallel())
        .subscribeWith(createSubscriber(receivePublisher, sendSink, sessionPublisher));
  }

  protected SessionSubscriber createSubscriber(
      Publisher<String> receivePublisher,
      FluxSink<String> receiveSink,
      Publisher<WebSocketSession> sessionPublisher) {
    Flux<String> receiveWithSessionClose =
        Flux.merge(
            receivePublisher,
            Flux.<String>defer(
                () ->
                    Flux.from(sessionPublisher)
                        .switchMap(
                            webSocketSession ->
                                Flux.<String>create(
                                    sink -> {
                                      // Empty, but won't complete until cancelled.
                                    })
                                    .doFinally(
                                        signal -> {
                                          log.info("Closing session.");
                                          webSocketSession.close();
                                        }))));

    return new SessionSubscriber(receiveWithSessionClose, receiveSink);
  }

  @RequiredArgsConstructor
  private static class SessionSubscriber implements Subscriber<Void> {
    @Getter private final Publisher<String> receivePublisher;
    @Getter private final FluxSink<String> sendSink;

    @Override
    public void onSubscribe(Subscription s) {
      // not used
    }

    @Override
    public void onNext(Void notused) {
      // not used
    }

    @Override
    public void onError(Throwable t) {
      // noop
    }

    @Override
    public void onComplete() {
      // noop
    }
  }
}
