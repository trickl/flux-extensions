package com.trickl.flux.websocket.stomp;

import com.trickl.flux.mappers.FluxSinkMapper;
import com.trickl.flux.websocket.BinaryWebSocketHandler;
import com.trickl.flux.websocket.WebSocketFluxClient;
import java.net.URI;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import lombok.Builder;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
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
  private Mono<Void> doBeforeOpen = Mono.empty();

  @Builder.Default private Function<FluxSink<StompFrame>, Mono<Void>> doAfterOpen
      = response -> Mono.empty();

  @Builder.Default
  private Function<Publisher<StompFrame>, Mono<Void>> doBeforeClose = response -> Mono.empty();

  @Builder.Default
  private Mono<Void> doAfterClose = Mono.empty();

  private final StompMessageCodec codec = new StompMessageCodec();

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
            .doBeforeOpen(doBeforeOpen)
            .doAfterOpen(sink -> doAfterOpen.apply(
                new FluxSinkMapper<StompFrame, byte[]>(sink, codec::encode)))
            .doBeforeClose(response -> 
               doBeforeClose.apply(stompInputTransformer.apply(response))
            )
            .doAfterClose(doAfterClose)
            .build();

    return stompInputTransformer
        .apply(
            webSocketFluxClient.get(
                stompOutputTransformer.apply(send)))
        .log("Raw Stomp Flux Client", Level.FINER);
  }
}
