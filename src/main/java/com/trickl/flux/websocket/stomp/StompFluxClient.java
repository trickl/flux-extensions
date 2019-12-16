package com.trickl.flux.websocket.stomp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.websocket.BinaryWebSocketFluxClient;
import com.trickl.flux.websocket.stomp.StompFrame;
import com.trickl.flux.websocket.stomp.frames.StompConnectFrame;
import com.trickl.flux.websocket.stomp.frames.StompDisconnectFrame;

import java.net.URI;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;

import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.client.WebSocketClient;

import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@RequiredArgsConstructor
public class StompFluxClient<O, I> {
  private final WebSocketClient webSocketClient;
  private final URI transportUri;
  private final Supplier<HttpHeaders> webSocketHeadersProvider;
  private final ObjectMapper objectMapper;

  private final Class<O> requestMessageType;
  private final Class<I> responseMessageType;

  /**
   * Connect to a stomp service.
   *
   * @param send The stream of messages to send.
   * @return A stream of messages received.
   */
  public Flux<StompFrame> get(Publisher<StompFrame> send) {
    StompInputTransformer<I> stompInputTransformer =
        new StompInputTransformer<>(
            objectMapper, responseMessageType);
    StompOutputTransformer<O> stompOutputTransformer =
        new StompOutputTransformer<>(objectMapper, requestMessageType);

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
    return stompInputTransformer.apply(webSocketFluxClient.get(
      stompOutputTransformer.apply(output)));
  }

  protected void onConnect(FluxSink<StompFrame> frameSink) {
    StompConnectFrame connectFrame = StompConnectFrame.builder()
        .acceptVersion("1.0,1.1,1.2")
        .host(transportUri.getHost())
        .build();
    frameSink.next(connectFrame);
  }

  protected void onDisconnect(FluxSink<StompFrame> frameSink) {
    StompDisconnectFrame disconnectFrame = StompDisconnectFrame.builder()
        .build();
    frameSink.next(disconnectFrame);
  }
}
