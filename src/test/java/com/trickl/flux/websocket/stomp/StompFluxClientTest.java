package com.trickl.flux.websocket.stomp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.config.WebSocketConfiguration;
import com.trickl.flux.websocket.MockServerWithWebSocket;
import com.trickl.flux.websocket.VerifierComplete;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.regex.Pattern;
import lombok.extern.java.Log;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@Log
@ActiveProfiles({"unittest"})
@TestPropertySource(properties = { "spring.config.location=classpath:application.yml" })
@SpringBootTest(classes = WebSocketConfiguration.class)
public class StompFluxClientTest {

  @Autowired ObjectMapper objectMapper = new ObjectMapper();

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
  private static final String STOMP_CONNECTED_NO_HB_MESSAGE = 
      "CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\u0000";
  private static final String STOMP_RECEIPT_MESSAGE = 
      "RECEIPT\nreceipt-id:message-12345\n\n\u0000";
  private static final String STOMP_MESSAGE = 
      "MESSAGE\nsubscription: 0\nmessage-id:001\ndestination:/messages\n" 
      + "content-type:text/plain\n\n\n\"hello\"\n\n\u0000";

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
  public void testConnectSubscribeDisconnect() {

    MockServerWithWebSocket mockServer = new MockServerWithWebSocket();

    WebSocketClient client = new ReactorNettyWebSocketClient();
    StompFluxClient stompClient =
        StompFluxClient.builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .doBeforeSessionOpen(Mono.defer(() -> {
          mockServer.start();          
          return Mono.delay(Duration.ofMillis(500)).then();
        }))
        .doAfterSessionClose(Mono.defer(() -> shutdown(mockServer)))
        .build();

    VerifierComplete verifierComplete = mockServer.beginVerifier()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()
        .thenExpectMessage(STOMP_CONNECT_PATTERN)
        .thenSend(STOMP_CONNECTED_NO_HB_MESSAGE)
        .thenExpectMessage(STOMP_SUBSCRIBE_PATTERN)
        .thenExpectMessage(STOMP_DISCONNECT_PATTERN)
        .thenSend(STOMP_RECEIPT_MESSAGE)
        .thenExpectClose(Duration.ofSeconds(3))
        .thenWaitServerShutdown()
        .thenVerify();

    Flux<String> output = stompClient.get(
        "/messages", String.class, Duration.ofMinutes(30));

    StepVerifier.create(output)
        .thenAwait(Duration.ofSeconds(5))      
        .thenCancel()        
        .verify(Duration.ofSeconds(30));

    verifierComplete.waitComplete(Duration.ofSeconds(30));
  }

  @Test
  public void testTimeoutDisconnectReceipt() {

    MockServerWithWebSocket mockServer = new MockServerWithWebSocket();

    WebSocketClient client = new ReactorNettyWebSocketClient();
    StompFluxClient stompClient =
        StompFluxClient.builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .doBeforeSessionOpen(Mono.defer(() -> {
          mockServer.start();          
          return Mono.delay(Duration.ofMillis(500)).then();
        }))
        .doAfterSessionClose(Mono.defer(() -> shutdown(mockServer)))
        .build();

    VerifierComplete verifierComplete = mockServer.beginVerifier()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()
        .thenExpectMessage(STOMP_CONNECT_PATTERN)
        .thenSend(STOMP_CONNECTED_NO_HB_MESSAGE)
        .thenExpectMessage(STOMP_SUBSCRIBE_PATTERN)
        .thenExpectMessage(STOMP_DISCONNECT_PATTERN)
        .thenExpectClose(Duration.ofSeconds(3))
        .thenWaitServerShutdown()
        .thenVerify();

    Flux<String> output = stompClient.get(
        "/messages", String.class, Duration.ofMinutes(30));

    StepVerifier.create(output)
        .thenAwait(Duration.ofSeconds(5))      
        .thenCancel()        
        .verify(Duration.ofSeconds(30));

    verifierComplete.waitComplete(Duration.ofSeconds(30));
  }

  @Test
  public void testMultipleSubscribers() {

    MockServerWithWebSocket mockServer = new MockServerWithWebSocket();

    WebSocketClient client = new ReactorNettyWebSocketClient();
    StompFluxClient stompClient =
        StompFluxClient.builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .doBeforeSessionOpen(Mono.defer(() -> {
          mockServer.start();          
          return Mono.delay(Duration.ofMillis(500)).then();
        }))
        .doAfterSessionClose(Mono.defer(() -> shutdown(mockServer)))
        .build();

    AtomicReference<Disposable> secondSubscription = new AtomicReference<>();

    VerifierComplete verifierComplete = mockServer.beginVerifier()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()
        .thenExpectMessage(STOMP_CONNECT_PATTERN)
        .thenSend(STOMP_CONNECTED_NO_HB_MESSAGE)
        .thenExpectMessage(STOMP_SUBSCRIBE_PATTERN)        
        .thenWait(Duration.ofSeconds(3))
        .then(() -> {
          Flux<String> output2 = stompClient.get(
              "/messages2", String.class, Duration.ofMinutes(30));
          secondSubscription.set(output2.subscribe());
        })
        .thenExpectMessage(STOMP_SUBSCRIBE_PATTERN)
        .thenWait(Duration.ofSeconds(1))
        .then(() -> {
          secondSubscription.get().dispose();
        })
        .thenExpectMessage(STOMP_UNSUBSCRIBE_PATTERN)
        .thenExpectMessage(STOMP_DISCONNECT_PATTERN)
        .thenSend(STOMP_RECEIPT_MESSAGE)
        .thenExpectClose(Duration.ofSeconds(3))
        .thenWaitServerShutdown()
        .thenVerify();

    Flux<String> output = stompClient.get(
        "/messages", String.class, Duration.ofSeconds(60));

    StepVerifier.create(output)
        .thenAwait(Duration.ofSeconds(10))      
        .thenCancel()        
        .verify(Duration.ofSeconds(30));

    verifierComplete.waitComplete(Duration.ofSeconds(30));
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

    Flux<String> output = stompClient.get(
        "/messages", String.class, Duration.ofMinutes(30));

    StepVerifier.create(output)
        .expectErrorMessage("Max retries exceeded")
        .verify(Duration.ofSeconds(30));
  }

  @Test
  public void testNoHeartbeatRetry() {

    MockServerWithWebSocket mockServer = new MockServerWithWebSocket();

    mockServer.beginVerifier()
        .thenWaitServerStartThenUpgrade(Duration.ofMinutes(5))
        .thenExpectOpen(Duration.ofMinutes(5))
        .thenExpectMessage(STOMP_CONNECT_PATTERN, Duration.ofMinutes(5))
        .thenSend(STOMP_CONNECTED_MESSAGE)        
        .thenExpectMessage(STOMP_SUBSCRIBE_PATTERN, Duration.ofMinutes(5))
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN, Duration.ofMinutes(5))
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN, Duration.ofMinutes(5))
        .thenExpectMessage(STOMP_ERROR_PATTERN, Duration.ofMinutes(5))
        .thenExpectMessage(STOMP_DISCONNECT_PATTERN, Duration.ofMinutes(5))
        .thenSend(STOMP_RECEIPT_MESSAGE)
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()        
        .thenExpectMessage(STOMP_CONNECT_PATTERN)
        .thenSend(STOMP_CONNECTED_MESSAGE)
        .thenExpectMessage(STOMP_SUBSCRIBE_PATTERN)
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN)
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN)
        .thenExpectMessage(STOMP_ERROR_PATTERN, Duration.ofMinutes(5))
        .thenExpectMessage(STOMP_DISCONNECT_PATTERN)
        .thenSend(STOMP_RECEIPT_MESSAGE)
        .thenExpectClose()
        .thenWaitServerShutdown()
        .thenWaitServerStartThenUpgrade()    
        .thenExpectOpen()        
        .thenExpectMessage(STOMP_CONNECT_PATTERN)
        .thenSend(STOMP_CONNECTED_MESSAGE)                
        .thenExpectMessage(STOMP_SUBSCRIBE_PATTERN)
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN)
        .thenExpectMessage(STOMP_HEARTBEAT_PATTERN)
        .thenExpectMessage(STOMP_ERROR_PATTERN, Duration.ofMinutes(5))
        .thenExpectMessage(STOMP_DISCONNECT_PATTERN)
        .thenSend(STOMP_RECEIPT_MESSAGE)
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
          log.info("Starting websocket session.");
          mockServer.start();          
          return Mono.delay(Duration.ofMillis(500)).then();
        }))
        .doAfterSessionClose(Mono.defer(() -> shutdown(mockServer)))
        .build();

    Flux<String> output = stompClient.get(
        "/messages", String.class, Duration.ofMinutes(30));

    StepVerifier.create(output)     
        .expectErrorMessage("Max retries exceeded")
        .verify(Duration.ofMinutes(1));
  }

  @Test
  public void testSubscribeMessage() {

    MockServerWithWebSocket mockServer = new MockServerWithWebSocket();

    VerifierComplete verifierComplete = mockServer.beginVerifier()
        .thenWaitServerStartThenUpgrade(Duration.ofMinutes(5))
        .thenExpectOpen(Duration.ofMinutes(5))
        .thenExpectMessage(STOMP_CONNECT_PATTERN, Duration.ofMinutes(5))
        .thenSend(STOMP_CONNECTED_NO_HB_MESSAGE)        
        .thenExpectMessage(STOMP_SUBSCRIBE_PATTERN, Duration.ofMinutes(5))
        .thenSend(STOMP_MESSAGE)
        .thenExpectMessage(STOMP_DISCONNECT_PATTERN, Duration.ofMinutes(5))
        .thenSend(STOMP_RECEIPT_MESSAGE)
        .thenExpectClose()
        .thenWaitServerShutdown()       
        .thenVerify(); 

    WebSocketClient client = new ReactorNettyWebSocketClient();
    StompFluxClient stompClient =
        StompFluxClient.builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .connectionTimeout(Duration.ofSeconds(15))
        .heartbeatReceiveFrequency(Duration.ofSeconds(0))
        .maxRetries(2)
        .doBeforeSessionOpen(Mono.defer(() -> {
          log.info("Starting websocket session.");
          mockServer.start();          
          return Mono.delay(Duration.ofMillis(500)).then();
        }))
        .doAfterSessionClose(Mono.defer(() -> shutdown(mockServer)))
        .build();

    Flux<String> output = stompClient.get(
        "/messages", String.class, Duration.ofSeconds(60))
        .doOnNext((String message) -> {
          log.log(Level.INFO, "\u001B[32mMESSAGE  ↑ {0}\u001B[0m", new Object[] {message});
        });

    StepVerifier.create(output, 256)
        .expectNext("hello")        
        .expectNoEvent(Duration.ofSeconds(5))
        .thenCancel()                 
        .verify(Duration.ofSeconds(60));

    verifierComplete.waitComplete(Duration.ofSeconds(60));
  }
}
