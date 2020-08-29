package com.trickl.flux.websocket.sockjs;

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
public class SockJsFluxClientTest {

  @Autowired ObjectMapper objectMapper = new ObjectMapper();

  private Subscription subscription;

  private static final Pattern SOCKJS_HEARTBEAT_PATTERN = Pattern.compile("h", Pattern.DOTALL);
  //private static final Pattern SOCKJS_SUBSCRIBE_PATTERN = 
  //    Pattern.compile("SUBSCRIBE.*", Pattern.DOTALL);
  private static final String SOCKJS_CONNECTED_MESSAGE = 
      "o";

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
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()                
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenWaitServerStartThenUpgrade()    
        .thenExpectOpen()                
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenVerify(); 

    WebSocketClient client = new ReactorNettyWebSocketClient();
    SockJsFluxClient sockJsClient =
        SockJsFluxClient.builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .connectionTimeout(Duration.ofSeconds(1))
        .objectMapper(objectMapper)
        .doBeforeSessionOpen(Mono.defer(() -> {
          mockServer.start();          
          return Mono.delay(Duration.ofMillis(500)).then();
        }))
        .maxRetries(2)
        .doAfterSessionClose(Mono.defer(() -> shutdown(mockServer)))
        .build();

    Flux<String> output = sockJsClient.get(
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
        .thenWaitServerStartThenUpgrade(Duration.ofMinutes(5))
        .thenExpectOpen(Duration.ofMinutes(5))
        .thenSend(SOCKJS_CONNECTED_MESSAGE)        
        .thenExpectMessage(SOCKJS_HEARTBEAT_PATTERN, Duration.ofMinutes(5))
        .thenExpectMessage(SOCKJS_HEARTBEAT_PATTERN, Duration.ofMinutes(5))
        .thenExpectMessage(SOCKJS_HEARTBEAT_PATTERN, Duration.ofMinutes(5))
        //.thenExpectMessage(SOCKJS_DISCONNECT_PATTERN, Duration.ofMinutes(5))
        //.thenSend(SOCKJS_RECEIPT_MESSAGE)
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()        
        .thenSend(SOCKJS_CONNECTED_MESSAGE)
        .thenExpectMessage(SOCKJS_HEARTBEAT_PATTERN)
        .thenExpectMessage(SOCKJS_HEARTBEAT_PATTERN)
        .thenExpectMessage(SOCKJS_HEARTBEAT_PATTERN)     
        //.thenExpectMessage(SOCKJS_DISCONNECT_PATTERN)
        //.thenSend(SOCKJS_RECEIPT_MESSAGE)
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenWaitServerStartThenUpgrade()    
        .thenExpectOpen()        
        .thenSend(SOCKJS_CONNECTED_MESSAGE)         
        .thenExpectMessage(SOCKJS_HEARTBEAT_PATTERN)
        .thenExpectMessage(SOCKJS_HEARTBEAT_PATTERN)
        .thenExpectMessage(SOCKJS_HEARTBEAT_PATTERN)
        //.thenExpectMessage(SOCKJS_DISCONNECT_PATTERN)
        //.thenSend(SOCKJS_RECEIPT_MESSAGE)
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenVerify(); 

    WebSocketClient client = new ReactorNettyWebSocketClient();
    SockJsFluxClient sockJsClient =
        SockJsFluxClient.builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .objectMapper(objectMapper)
        .connectionTimeout(Duration.ofSeconds(15))
        .heartbeatReceiveFrequency(Duration.ofMillis(7500))
        .heartbeatSendFrequency(Duration.ofSeconds(3))
        .maxRetries(2)
        .doBeforeSessionOpen(Mono.defer(() -> {
          mockServer.start();          
          return Mono.delay(Duration.ofMillis(500)).then();
        }))
        .doAfterSessionClose(Mono.defer(() -> shutdown(mockServer)))
        .build();

    Flux<String> output = sockJsClient.get(
        "/messages", String.class, Duration.ofMinutes(30));

    StepVerifier.create(output)
        .consumeSubscriptionWith(sub -> subscription = sub)        
        .expectErrorMessage("Max retries exceeded")
        .verify(Duration.ofMinutes(30));
  }
}
