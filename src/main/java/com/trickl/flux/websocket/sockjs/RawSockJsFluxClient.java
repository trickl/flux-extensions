package com.trickl.flux.websocket.sockjs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.websocket.TextWebSocketFluxClient;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import org.springframework.web.socket.sockjs.client.SockJsUrlInfo;
import org.springframework.web.socket.sockjs.transport.TransportType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class RawSockJsFluxClient {
  private final WebSocketClient webSocketClient;
  private final SockJsUrlInfo sockJsUrlInfo;
  private final Mono<HttpHeaders> webSocketHeadersProvider;
  private final ObjectMapper objectMapper;
  private final Supplier<String> openMessageSupplier;
  private final Supplier<String> hearbeatMessageSupplier;
  private final Function<CloseStatus, String> closeMessageFunction;

  /**
   * Connect to a sockjs service.
   *
   * @param send The stream of messages to send.
   * @return A stream of messages received.
   */
  public Flux<String> get(Publisher<String> send) {
    SockJsInputTransformer sockJsInputTransformer =
        new SockJsInputTransformer(
            objectMapper, openMessageSupplier, hearbeatMessageSupplier, closeMessageFunction);
    SockJsOutputTransformer sockJsOutputTransformer = new SockJsOutputTransformer(objectMapper);

    TextWebSocketFluxClient webSocketFluxClient =
        new TextWebSocketFluxClient(
            webSocketClient,
            sockJsUrlInfo.getTransportUrl(TransportType.WEBSOCKET),
            webSocketHeadersProvider);

    return sockJsInputTransformer.apply(
        webSocketFluxClient.get(sockJsOutputTransformer.apply(Flux.from(send))));
  }
}
