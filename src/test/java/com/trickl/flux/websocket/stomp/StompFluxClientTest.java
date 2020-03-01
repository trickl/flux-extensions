package com.trickl.flux.websocket.stomp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.config.WebSocketConfiguration;
import com.trickl.flux.websocket.MockServerWithWebSocket;

import java.io.IOException;
import java.time.Duration;
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
import reactor.test.StepVerifier;

@ActiveProfiles({"unittest"})
@TestPropertySource(properties = { "spring.config.location=classpath:application.yml" })
@SpringBootTest(classes = WebSocketConfiguration.class)
public class StompFluxClientTest {

  @Autowired ObjectMapper objectMapper = new ObjectMapper();

  private Subscription subscription;

  private static final Pattern STOMP_CONNECT_PATTERN = Pattern.compile("CONNECT.*", Pattern.DOTALL);
  private static final Pattern STOMP_DISCONNECT_PATTERN 
      = Pattern.compile("DISCONNECT.*", Pattern.DOTALL);

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

  @Test
  public void testConnectionRetry() {

    MockServerWithWebSocket mockServer = new MockServerWithWebSocket();

    mockServer.beginVerifier()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()
        .thenExpectMessage(STOMP_CONNECT_PATTERN)
        .thenExpectMessage(STOMP_DISCONNECT_PATTERN)
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()        
        .thenExpectMessage(STOMP_CONNECT_PATTERN)
        .thenExpectMessage(STOMP_DISCONNECT_PATTERN)
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenWaitServerStartThenUpgrade()    
        .thenExpectOpen()        
        .thenExpectMessage(STOMP_CONNECT_PATTERN)
        .thenExpectMessage(STOMP_DISCONNECT_PATTERN)
        .thenExpectClose()
        .thenVerify(); 

    WebSocketClient client = new ReactorNettyWebSocketClient();
    StompFluxClient stompClient =
        StompFluxClient.builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .connectionTimeout(Duration.ofSeconds(1))
        .beforeConnect(mockServer::start)
        .afterDisconnect(() -> {
          try {
            mockServer.shutdown();
          } catch (IOException ex) {
            throw new IllegalStateException("Unable to shutdown server", ex);
          }
        })
        .build();

    Flux<String> output = stompClient.subscribe(
        "/messages", String.class, Duration.ofMinutes(30));

    StepVerifier.create(output)
        .consumeSubscriptionWith(sub -> subscription = sub)
        .expectError()
        //.then(this::unsubscribe)
        //.expectComplete()
        .verify();
  }
}
