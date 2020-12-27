package com.trickl.flux.websocket.sockjs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.config.WebSocketConfiguration;
import com.trickl.flux.websocket.MockServerWithWebSocket;
import com.trickl.flux.websocket.sockjs.frames.SockJsMessageFrame;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ActiveProfiles({"unittest"})
@TestPropertySource(properties = { "spring.config.location=classpath:application.yml" })
@SpringBootTest(classes = WebSocketConfiguration.class)
public class SockJsFluxClientTest {

  @Autowired ObjectMapper objectMapper = new ObjectMapper();

  private Subscription subscription;

  //private static final Pattern SOCKJS_HEARTBEAT_PATTERN = Pattern.compile("h", Pattern.DOTALL);
  private static final String SOCKJS_CONNECTED_MESSAGE = 
      "o";
  private static final Pattern TEST_SUBSCRIBE_PATTERN = 
      Pattern.compile("SUBSCRIBE", Pattern.DOTALL);
  private static final Pattern TEST_UNSUBSCRIBE_PATTERN = 
      Pattern.compile("UNSUBSCRIBE", Pattern.DOTALL);  

  @BeforeEach
  private void setup() {      
    subscription = null;
  }

  void unsubscribe() {
    if (subscription != null) {
      subscription.cancel();
    }
    subscription = null;
  }

  Mono<Void> shutdown(MockServerWithWebSocket mockServer) {
    return Mono.delay(Duration.ofMillis(500)).then(
      Mono.create(sink -> {
        try {
          mockServer.shutdown();
        } catch (IOException ex) {
          sink.error(new IllegalStateException("Unable to shutdown server", ex));
        }
        sink.success();    
      })
    ).then(Mono.delay(Duration.ofMillis(500))).then();
  }

  @Test
  public void testSubscribe() {

    MockServerWithWebSocket mockServer = new MockServerWithWebSocket();

    mockServer.beginVerifier()
        .thenWaitServerStartThenUpgrade(Duration.ofMinutes(5))
        .thenExpectOpen(Duration.ofMinutes(5))
        .thenSend(SOCKJS_CONNECTED_MESSAGE)        
        .thenExpectMessage(TEST_SUBSCRIBE_PATTERN)
        .thenExpectMessage(TEST_UNSUBSCRIBE_PATTERN)
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenVerify(); 

    WebSocketClient client = new ReactorNettyWebSocketClient();
    SockJsFluxClient<String, String, String> sockJsClient =
        SockJsFluxClient.<String, String, String>builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .objectMapper(objectMapper)
        .connectionTimeout(Duration.ofSeconds(15))
        .heartbeatReceiveFrequency(Duration.ZERO)
        .heartbeatSendFrequency(Duration.ZERO)
        .buildSubscribeFrames((added, all) -> {
          return Arrays.asList(SockJsMessageFrame.builder()
            .message("{action: \"SUBSCRIBE\"}")
            .build());
        })
        .buildUnsubscribeFrames((removed, all) -> {
          return Arrays.asList(SockJsMessageFrame.builder()
            .message("{action: \"UNSUBSCRIBE\"}")
            .build());
        })
        .maxRetries(2)
        .doBeforeSessionOpen(Mono.defer(() -> {
          mockServer.start();          
          return Mono.delay(Duration.ofMillis(500)).then();
        }))
        .doAfterSessionClose(Mono.defer(() -> shutdown(mockServer)))
        .build();

    Flux<String> output = sockJsClient.get(
        "/messages", String.class, Duration.ofMinutes(30), Flux.empty());

    StepVerifier.create(output)
        .thenAwait(Duration.ofSeconds(5))      
        .thenCancel()        
        .verify(Duration.ofSeconds(30));
  }
}
