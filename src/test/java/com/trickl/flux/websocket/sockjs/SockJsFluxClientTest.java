package com.trickl.flux.websocket.sockjs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.config.WebSocketConfiguration;
import com.trickl.flux.websocket.MockServerWithWebSocket;
import com.trickl.flux.websocket.VerifierComplete;
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
  private static final String ECHO_MESSAGE = 
      "a[\"ECHO\"]";
  private static final String CLOSE_MESSAGE = 
      "a[\"CLOSE\"]";
  private static final Pattern SUBSCRIBE_PATTERN = 
      Pattern.compile(Pattern.quote("[\"SUBSCRIBE\"]"), Pattern.DOTALL);
  private static final Pattern UNSUBSCRIBE_PATTERN = 
      Pattern.compile(Pattern.quote("[\"UNSUBSCRIBE\"]"), Pattern.DOTALL);  
  private static final Pattern ECHO_RESPONSE_PATTERN = 
      Pattern.compile("\\[\\\"\\\\\\\"ECHO-ECHO\\\\\\\"\\\"\\]", Pattern.DOTALL);  

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

    VerifierComplete verifierComplete = mockServer.beginVerifier()
        .then(() -> mockServer.start())
        .thenWaitServerStartThenUpgrade(Duration.ofMinutes(5))
        .thenExpectOpen(Duration.ofMinutes(5))
        .thenSend(SOCKJS_CONNECTED_MESSAGE)        
        .thenExpectMessage(SUBSCRIBE_PATTERN)
        .thenExpectMessage(UNSUBSCRIBE_PATTERN)
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
            .message("SUBSCRIBE")
            .build());
        })
        .buildUnsubscribeFrames((removed, all) -> {
          return Arrays.asList(SockJsMessageFrame.builder()
            .message("UNSUBSCRIBE")
            .build());
        })
        .maxRetries(2)
        .doAfterSessionClose(Mono.defer(() -> shutdown(mockServer)))
        .build();

    Flux<String> output = sockJsClient.get(
        "/messages", String.class, Duration.ofMinutes(30), Flux.empty());

    StepVerifier.create(output)
        .thenAwait(Duration.ofSeconds(5))      
        .thenCancel()        
        .verify(Duration.ofSeconds(30));

    verifierComplete.waitComplete(Duration.ofSeconds(10));
  }

  @Test
  public void testEcho() {

    MockServerWithWebSocket mockServer = new MockServerWithWebSocket();

    VerifierComplete verifierComplete = mockServer.beginVerifier()
        .then(() -> mockServer.start())
        .thenWaitServerStartThenUpgrade(Duration.ofMinutes(5))
        .thenExpectOpen(Duration.ofMinutes(5))
        .thenSend(SOCKJS_CONNECTED_MESSAGE)        
        .thenSend(ECHO_MESSAGE)
        .thenExpectMessage(ECHO_RESPONSE_PATTERN)
        .thenExpectClose()
        .then(() -> shutdown(mockServer).subscribe())
        .thenWaitServerShutdown()
        .thenVerify(); 

    WebSocketClient client = new ReactorNettyWebSocketClient();
    SockJsFluxClient<String, String, String> sockJsClient =
        SockJsFluxClient.<String, String, String>builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .objectMapper(objectMapper)
        .connectionTimeout(Duration.ofSeconds(15))
        .responseClazz(String.class)
        .heartbeatReceiveFrequency(Duration.ZERO)
        .heartbeatSendFrequency(Duration.ZERO)
        .handleProtocolFrames((message, send, onComplete) -> {
          if ("ECHO".equals(message)) {
            send.tryEmitNext("ECHO-ECHO");
          }
        })
        .maxRetries(2)  
        .build();

    Flux<String> output = sockJsClient.get(
        "/messages", String.class, Duration.ofMinutes(30), Flux.empty());
        
    StepVerifier.create(output)
        .thenAwait(Duration.ofSeconds(5))      
        .thenCancel()        
        .verify(Duration.ofSeconds(30));

    verifierComplete.waitComplete(Duration.ofSeconds(10));
  }

  @Test
  public void testProtocolClose() {

    MockServerWithWebSocket mockServer = new MockServerWithWebSocket();

    VerifierComplete verifierComplete = mockServer.beginVerifier()
        .then(() -> mockServer.start())
        .thenWaitServerStartThenUpgrade(Duration.ofMinutes(5))
        .thenExpectOpen(Duration.ofMinutes(5))
        .thenSend(SOCKJS_CONNECTED_MESSAGE)        
        .thenSend(ECHO_MESSAGE)
        .thenSend(CLOSE_MESSAGE)
        .thenExpectClose()
        .then(() -> shutdown(mockServer).subscribe())
        .thenWaitServerShutdown()
        .thenVerify(); 

    WebSocketClient client = new ReactorNettyWebSocketClient();
    SockJsFluxClient<String, String, String> sockJsClient =
        SockJsFluxClient.<String, String, String>builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .objectMapper(objectMapper)
        .connectionTimeout(Duration.ofSeconds(15))
        .responseClazz(String.class)
        .heartbeatReceiveFrequency(Duration.ZERO)
        .heartbeatSendFrequency(Duration.ZERO)
        .handleProtocolFrames((message, send, onComplete) -> {
          if ("CLOSE".equals(message)) {
            onComplete.run();
          }
        })
        .maxRetries(2)  
        .build();

    Flux<String> output = sockJsClient.get(
        "/messages", String.class, Duration.ofMinutes(30), Flux.empty());
        
    StepVerifier.create(output)
        .thenAwait(Duration.ofSeconds(20))      
        .thenCancel()        
        .verify(Duration.ofSeconds(30));

    verifierComplete.waitComplete(Duration.ofSeconds(10));
  }
}
