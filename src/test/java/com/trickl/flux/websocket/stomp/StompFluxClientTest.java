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
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ActiveProfiles({"unittest"})
@TestPropertySource(properties = { "spring.config.location=classpath:application.yml" })
@SpringBootTest(classes = WebSocketConfiguration.class)
public class StompFluxClientTest {

  @Autowired ObjectMapper objectMapper = new ObjectMapper();

  private Subscription subscription;

  private static final Pattern STOMP_CONNECT_PATTERN = Pattern.compile("CONNECT.*", Pattern.DOTALL);
  private static final Pattern STOMP_HEARTBEAT_PATTERN = Pattern.compile("\\s*", Pattern.DOTALL);
  private static final Pattern STOMP_SUBSCRIBE_PATTERN = 
      Pattern.compile("SUBSCRIBE.*", Pattern.DOTALL);
  private static final Pattern STOMP_UNSUBSCRIBE_PATTERN = 
      Pattern.compile("UNSUBSCRIBE.*", Pattern.DOTALL);
  private static final Pattern STOMP_ERROR_PATTERN = 
      Pattern.compile("ERROR.*", Pattern.DOTALL);
  private static final Pattern STOMP_DISCONNECT_PATTERN 
      = Pattern.compile("DISCONNECT.*", Pattern.DOTALL);
  private static final String STOMP_CONNECTED_MESSAGE = 
      "CONNECTED\nversion:1.2\nheart-beat:3000,3000\n\n\u0000";
  private static final String STOMP_RECEIPT_MESSAGE = 
      "RECEIPT\nreceipt-id:message-12345\n\n\u0000";

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
  public void testNoConnectionRetry() {

    MockServerWithWebSocket mockServer = new MockServerWithWebSocket();

    mockServer.beginVerifier()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()
        .thenExpectMessage(STOMP_CONNECT_PATTERN)
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()        
        .thenExpectMessage(STOMP_CONNECT_PATTERN)
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenWaitServerStartThenUpgrade()    
        .thenExpectOpen()        
        .thenExpectMessage(STOMP_CONNECT_PATTERN)
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenVerify(); 

    WebSocketClient client = new ReactorNettyWebSocketClient();
    StompFluxClient stompClient =
        StompFluxClient.builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .connectionTimeout(Duration.ofSeconds(1))
        .doBeforeSessionOpen(Mono.defer(() -> {
          mockServer.start();          
          return Mono.delay(Duration.ofMillis(500)).then();
        }))
        .maxRetries(2)
        .doAfterSessionClose(Mono.defer(() -> shutdown(mockServer)))
        .build();

    Flux<String> output = stompClient.subscribe(
        "/messages", String.class, Duration.ofMinutes(30));

    StepVerifier.create(output)
        .consumeSubscriptionWith(sub -> subscription = sub)
        .expectErrorMessage("Max retries exceeded")
        .verify(Duration.ofSeconds(30));
  }

  @Test
  public void testNoHeartbeatRetry() {

    MockServerWithWebSocket mockServer = new MockServerWithWebSocket();

    mockServer.beginVerifier()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()
        .thenExpectMessage(STOMP_CONNECT_PATTERN)
        .thenSend(STOMP_CONNECTED_MESSAGE)        
        .thenExpectMessage(STOMP_SUBSCRIBE_PATTERN)
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN)
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN)
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN)
        .thenExpectMessage(STOMP_ERROR_PATTERN)
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()        
        .thenExpectMessage(STOMP_CONNECT_PATTERN)
        .thenSend(STOMP_CONNECTED_MESSAGE)
        .thenExpectMessage(STOMP_SUBSCRIBE_PATTERN)
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN)
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN)
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN)     
        .thenExpectMessage(STOMP_ERROR_PATTERN)
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenWaitServerStartThenUpgrade()    
        .thenExpectOpen()        
        .thenExpectMessage(STOMP_CONNECT_PATTERN)
        .thenSend(STOMP_CONNECTED_MESSAGE)                
        .thenExpectMessage(STOMP_SUBSCRIBE_PATTERN)
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN)
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN)
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN)       
        .thenExpectMessage(STOMP_ERROR_PATTERN)
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenVerify(); 

    WebSocketClient client = new ReactorNettyWebSocketClient();
    StompFluxClient stompClient =
        StompFluxClient.builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .connectionTimeout(Duration.ofSeconds(15))
        .heartbeatReceiveFrequency(Duration.ofSeconds(3))
        .maxRetries(2)
        .doBeforeSessionOpen(Mono.defer(() -> {
          mockServer.start();          
          return Mono.delay(Duration.ofMillis(500)).then();
        }))
        .doAfterSessionClose(Mono.defer(() -> shutdown(mockServer)))
        .build();

    Flux<String> output = stompClient.subscribe(
        "/messages", String.class, Duration.ofMinutes(30));

    StepVerifier.create(output)
        .consumeSubscriptionWith(sub -> subscription = sub)        
        .expectErrorMessage("Max retries exceeded")
        .verify(Duration.ofMinutes(30));
  }
}
