package com.trickl.flux.websocket.sockjs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.websocket.TextWebSocketHandler;
import com.trickl.flux.websocket.WebSocketFluxClient;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import lombok.Builder;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import org.springframework.web.socket.sockjs.client.SockJsUrlInfo;
import org.springframework.web.socket.sockjs.transport.TransportType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Builder
public class RawSockJsFluxClient {
  private final WebSocketClient webSocketClient;
  private final SockJsUrlInfo sockJsUrlInfo;

  public static final String SOCK_JS_OPEN = "o";
  public static final String SOCK_JS_CLOSE = "c";
  public static final String SOCK_JS_HEARTBEAT = "h";
  
  private final ObjectMapper objectMapper;
  @Builder.Default private Supplier<String> openMessageSupplier = () -> SOCK_JS_OPEN;
  @Builder.Default private Supplier<String> hearbeatMessageSupplier = () -> SOCK_JS_HEARTBEAT;
  @Builder.Default private Function<CloseStatus, String> closeMessageFunction 
      = (CloseStatus status) -> SOCK_JS_CLOSE + status.getCode();

  @Builder.Default private Mono<HttpHeaders> webSocketHeadersProvider 
      = Mono.fromSupplier(HttpHeaders::new);

  @Builder.Default
  private Mono<Void> doBeforeOpen = Mono.empty();

  @Builder.Default
  private Mono<Void> doAfterClose = Mono.empty();

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

    WebSocketFluxClient<String> webSocketFluxClient =
        WebSocketFluxClient.<String>builder()
            .webSocketClient(webSocketClient)
            .transportUriProvider(() -> sockJsUrlInfo.getTransportUrl(TransportType.WEBSOCKET))
            .handlerFactory(TextWebSocketHandler::new)
            .webSocketHeadersProvider(webSocketHeadersProvider)
            .doBeforeOpen(doBeforeOpen)
            .doAfterClose(doAfterClose)
            .build();

    return sockJsInputTransformer.apply(
        webSocketFluxClient.get(
          sockJsOutputTransformer.apply(
            Flux.defer(
                () -> Flux.from(send))))
            )
          .log("Raw Sock Js Flux Client", Level.FINER);
  }
}
