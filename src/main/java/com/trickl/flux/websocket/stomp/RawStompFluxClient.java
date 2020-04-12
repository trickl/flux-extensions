package com.trickl.flux.websocket.stomp;

import com.trickl.flux.websocket.BinaryWebSocketFluxClient;
import com.trickl.flux.websocket.stomp.frames.StompConnectFrame;
import com.trickl.flux.websocket.stomp.frames.StompDisconnectFrame;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import lombok.Builder;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Log
@Builder
public class RawStompFluxClient {
  private final WebSocketClient webSocketClient;
  private final Supplier<URI> transportUriProvider;
  @Builder.Default private Supplier<HttpHeaders> webSocketHeadersProvider = HttpHeaders::new;
  @Builder.Default private Duration heartbeatSendFrequency = Duration.ofSeconds(5);
  @Builder.Default private Duration heartbeatReceiveFrequency = Duration.ofSeconds(5);
  @Builder.Default private Duration disconnectReceiptTimeout = Duration.ofMillis(500);

  @Builder.Default
  private Runnable beforeConnect =
      () -> {
        /* Noop */
      };

  @Builder.Default
  private Runnable afterDisconnect =
      () -> {
        /* Noop */
      };

  /**
   * Connect to a stomp service.
   *
   * @param send The stream of messages to send.
   * @return A stream of messages received.
   */
  public Flux<StompFrame> get(Publisher<StompFrame> send) {
    StompInputTransformer stompInputTransformer = new StompInputTransformer();
    StompOutputTransformer stompOutputTransformer = new StompOutputTransformer();

    BinaryWebSocketFluxClient webSocketFluxClient =
        BinaryWebSocketFluxClient.builder()
            .webSocketClient(webSocketClient)
            .transportUriProvider(transportUriProvider)
            .webSocketHeadersProvider(webSocketHeadersProvider)
            .beforeConnect(beforeConnect)
            .afterConnect(this::afterConnect)
            .beforeDisconnect(this::beforeDisconnect)
            .afterDisconnect(afterDisconnect)
            .build();
    return stompInputTransformer
        .apply(
            webSocketFluxClient.get(
                stompOutputTransformer.apply(
                    Flux.defer(
                        () -> {
                          EmitterProcessor<StompFrame> frameProcessor = EmitterProcessor.create();
                          return Flux.merge(frameProcessor, send);
                        }))))
        .log("Raw Stomp Flux Client", Level.FINER);
  }

  protected void afterConnect(FluxSink<byte[]> sendSink) {
    log.info("Sending connect frame after connection");
    StompMessageCodec codec = new StompMessageCodec();
    StompConnectFrame connectFrame =
        StompConnectFrame.builder()
            .acceptVersion("1.0,1.1,1.2")
            .heartbeatSendFrequency(heartbeatSendFrequency)
            .heartbeatReceiveFrequency(heartbeatReceiveFrequency)
            .host(transportUriProvider.get().getHost())
            .build();
    try {
      byte[] encoded = codec.encode(connectFrame);
      sendSink.next(encoded);
    } catch (IOException ex) {
      log.log(Level.WARNING, "Bad connection frame encoding", ex);
    }
  }

  protected void beforeDisconnect(FluxSink<byte[]> sendSink) {
    CountDownLatch countDownLatch = new CountDownLatch(1);
    StompMessageCodec codec = new StompMessageCodec();
    StompDisconnectFrame disconnectFrame = StompDisconnectFrame.builder().build();
    try {
      byte[] encodedFrame = codec.encode(disconnectFrame);
      sendSink.next(encodedFrame);
      if (countDownLatch.await(disconnectReceiptTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
        log.info("No receipt for disconnect in time, closing socket anyway");
      }
    } catch (InterruptedException ex) {
      log.info("Interruped wait on disconnect receipt");
      Thread.currentThread().interrupt();
    } catch (IOException ex) {
      log.log(Level.WARNING, "Bad disconnection frame encoding", ex);
    }
  }
}
