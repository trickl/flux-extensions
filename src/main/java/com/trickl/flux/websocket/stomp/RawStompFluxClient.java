package com.trickl.flux.websocket.stomp;

import com.trickl.flux.websocket.BinaryWebSocketHandler;
import com.trickl.flux.websocket.WebSocketFluxClient;
import java.net.URI;
import java.time.Duration;
import java.util.function.Supplier;
import java.util.logging.Level;
import lombok.Builder;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Builder
public class RawStompFluxClient {
  private final WebSocketClient webSocketClient;
  private final Supplier<URI> transportUriProvider;
  @Builder.Default private Mono<HttpHeaders> webSocketHeadersProvider
      = Mono.fromSupplier(HttpHeaders::new);
  @Builder.Default private Duration heartbeatSendFrequency = Duration.ofSeconds(5);
  @Builder.Default private Duration heartbeatReceiveFrequency = Duration.ofSeconds(5);
  @Builder.Default private Duration disconnectReceiptTimeout = Duration.ofMillis(500);

  @Builder.Default
  private Mono<Void> doBeforeSessionOpen = Mono.empty();

  @Builder.Default
  private Mono<Void>  doAfterOpen = Mono.empty();

  @Builder.Default
  private Mono<Void> doBeforeClose = Mono.empty();

  @Builder.Default
  private Mono<Void> doAfterSessionClose = Mono.empty();

  /**
   * Connect to a stomp service.
   *
   * @param send The stream of messages to send.
   * @return A stream of messages received.
   */
  public Flux<StompFrame> get(Publisher<StompFrame> send) {
    StompInputTransformer stompInputTransformer = new StompInputTransformer();
    StompOutputTransformer stompOutputTransformer = new StompOutputTransformer();

    WebSocketFluxClient<byte[]> webSocketFluxClient =
        WebSocketFluxClient.<byte[]>builder()
            .webSocketClient(webSocketClient)
            .transportUriProvider(transportUriProvider)
            .handlerFactory(BinaryWebSocketHandler::new)
            .webSocketHeadersProvider(webSocketHeadersProvider)
            .doBeforeSessionOpen(doBeforeSessionOpen)
            .doAfterOpen(doAfterOpen)
            .doBeforeClose(doBeforeClose)
            .doAfterSessionClose(doAfterSessionClose)
            .build();

    return stompInputTransformer
        .apply(
            webSocketFluxClient.get(
                stompOutputTransformer.apply(send)))
        .log("Raw Stomp Flux Client", Level.FINER);
  }
}
