package com.trickl.flux.websocket.stomp;

import com.trickl.flux.websocket.BinaryWebSocketFluxClient;
import com.trickl.flux.websocket.stomp.frames.StompConnectFrame;
import com.trickl.flux.websocket.stomp.frames.StompDisconnectFrame;
import java.net.URI;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class RawStompFluxClient {
  private final WebSocketClient webSocketClient;
  private final URI transportUri;
  private final Mono<HttpHeaders> webSocketHeadersProvider;
  private final Duration heartbeatSendFrequency;
  private final Duration heartbeatReceiveFrequency;

  /**
   * Connect to a stomp service.
   *
   * @param send The stream of messages to send.
   * @return A stream of messages received.
   */
  public Flux<StompFrame> get(Publisher<StompFrame> send) {
    StompInputTransformer stompInputTransformer = new StompInputTransformer();
    StompOutputTransformer stompOutputTransformer = new StompOutputTransformer();

    EmitterProcessor<StompFrame> frameProcessor = EmitterProcessor.create();
    FluxSink<StompFrame> frameSink = frameProcessor.sink();
    Flux<StompFrame> output = Flux.merge(frameProcessor, send);

    BinaryWebSocketFluxClient webSocketFluxClient =
        new BinaryWebSocketFluxClient(
            webSocketClient,
            transportUri,
            webSocketHeadersProvider,
            () -> onConnect(frameSink),
            () -> onDisconnect(frameSink));
    return stompInputTransformer.apply(
        webSocketFluxClient.get(stompOutputTransformer.apply(output)));
  }

  protected void onConnect(FluxSink<StompFrame> frameSink) {
    StompConnectFrame connectFrame =
        StompConnectFrame.builder()
            .acceptVersion("1.0,1.1,1.2")
            .heartbeatSendFrequency(heartbeatSendFrequency)
            .heartbeatReceiveFrequency(heartbeatReceiveFrequency)
            .host(transportUri.getHost())
            .build();
    frameSink.next(connectFrame);
  }

  protected void onDisconnect(FluxSink<StompFrame> frameSink) {
    StompDisconnectFrame disconnectFrame = StompDisconnectFrame.builder().build();
    frameSink.next(disconnectFrame);
  }
}
