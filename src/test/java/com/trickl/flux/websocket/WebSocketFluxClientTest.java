package com.trickl.flux.websocket;

import static org.assertj.core.api.Assertions.assertThat; 

import com.trickl.flux.config.WebSocketConfiguration;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.java.Log;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

@ActiveProfiles({"unittest"})
@TestPropertySource(properties = { "spring.config.location=classpath:application.yml" })
@SpringBootTest(classes = WebSocketConfiguration.class)
@Log
public class WebSocketFluxClientTest {

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
  public void testConnectDisconnect() {

    MockServerWithWebSocket mockServer = new MockServerWithWebSocket();
    
    WebSocketClient client = new ReactorNettyWebSocketClient();
    WebSocketFluxClient<String> webSocketFluxClient =
        WebSocketFluxClient.<String>builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .handlerFactory(TextWebSocketHandler::new)
        .build();

    mockServer.start();
    VerifierComplete verifierComplete = mockServer.beginVerifier()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()
        .thenExpectClose(Duration.ofSeconds(3))
        .then(() -> shutdown(mockServer).subscribe())
        .thenWaitServerShutdown()
        .thenVerify();

    Flux<String> output = webSocketFluxClient.get(Flux.empty());

    StepVerifier.create(output)
        .thenAwait(Duration.ofSeconds(5))      
        .thenCancel()        
        .verify(Duration.ofSeconds(15));

    verifierComplete.waitComplete(Duration.ofSeconds(30));
  }

  @Test
  public void testConnectionHooks() {

    MockServerWithWebSocket mockServer = new MockServerWithWebSocket();

    Sinks.One<String> doBeforeOpen = Sinks.one();
    Sinks.One<String> doAfterOpen = Sinks.one();
    Sinks.One<String> doBeforeClose = Sinks.one();
    Sinks.One<String> doAfterClose = Sinks.one();
    
    WebSocketClient client = new ReactorNettyWebSocketClient();
    WebSocketFluxClient<String> webSocketFluxClient =
        WebSocketFluxClient.<String>builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .handlerFactory(TextWebSocketHandler::new)
        .doBeforeOpen(Mono.fromRunnable(() -> doBeforeOpen.tryEmitValue("BEFORE_OPEN")))
        .doAfterOpen(sink -> Mono.fromRunnable(() -> doAfterOpen.tryEmitValue("AFTER_OPEN")))
        .doBeforeClose(sink -> Mono.fromRunnable(() -> doBeforeClose.tryEmitValue("BEFORE_CLOSE")))
        .doAfterClose(Mono.fromRunnable(() -> doAfterClose.tryEmitValue("AFTER_CLOSE")))
        .build();


    List<String> hooksList = new ArrayList<>();
    Disposable hooksSubscription = Flux.merge(
        doBeforeOpen.asMono(),
        doAfterOpen.asMono(),
        doBeforeClose.asMono(),
        doAfterClose.asMono())
        .doOnNext(hooksList::add)
        .doOnNext(hook -> log.info("Encountered Hook - " + hook))
        .subscribe();

    mockServer.start();
    VerifierComplete verifierComplete = mockServer.beginVerifier()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()        
        .thenExpectClose(Duration.ofSeconds(3))
        .then(() -> shutdown(mockServer).subscribe())
        .thenWaitServerShutdown()
        .then(() -> hooksSubscription.dispose())
        .thenVerify();

    Flux<String> output = webSocketFluxClient.get(Flux.empty());

    StepVerifier.create(output)
        .thenAwait(Duration.ofSeconds(5))      
        .thenCancel()
        .verify(Duration.ofSeconds(15));    

    assertThat(hooksList).containsExactly(
        "BEFORE_OPEN",
        "AFTER_OPEN",
        "BEFORE_CLOSE",
        "AFTER_CLOSE"
    );

    verifierComplete.waitComplete(Duration.ofSeconds(30));
  }

  @Test
  public void testConnectionHooksWithServerClose() {

    MockServerWithWebSocket mockServer = new MockServerWithWebSocket();

    Sinks.One<String> doBeforeOpen = Sinks.one();
    Sinks.One<String> doAfterOpen = Sinks.one();
    Sinks.One<String> doBeforeClose = Sinks.one();
    Sinks.One<String> doAfterClose = Sinks.one();
    
    WebSocketClient client = new ReactorNettyWebSocketClient();
    WebSocketFluxClient<String> webSocketFluxClient =
        WebSocketFluxClient.<String>builder()
        .webSocketClient(client)
        .transportUriProvider(mockServer::getWebSocketUri)
        .handlerFactory(TextWebSocketHandler::new)
        .doBeforeOpen(Mono.fromRunnable(() -> doBeforeOpen.tryEmitValue("BEFORE_OPEN")))
        .doAfterOpen(sink -> Mono.fromRunnable(() -> doAfterOpen.tryEmitValue("AFTER_OPEN")))
        .doBeforeClose(sink -> Mono.fromRunnable(() -> doBeforeClose.tryEmitValue("BEFORE_CLOSE")))
        .doAfterClose(Mono.fromRunnable(() -> doAfterClose.tryEmitValue("AFTER_CLOSE")))
        .build();


    List<String> hooksList = new ArrayList<>();
    Disposable hooksSubscription = Flux.merge(
        doBeforeOpen.asMono(),
        doAfterOpen.asMono(),
        doBeforeClose.asMono(),
        doAfterClose.asMono())
        .doOnNext(hooksList::add)
        .doOnNext(hook -> log.info("Encountered Hook - " + hook))
        .subscribe();

    mockServer.start();
    VerifierComplete verifierComplete = mockServer.beginVerifier()
        .thenWaitServerStartThenUpgrade()
        .thenExpectOpen()        
        .thenWait(Duration.ofSeconds(2))
        .thenClose()
        .then(() -> shutdown(mockServer).subscribe())
        .thenWaitServerShutdown()
        .then(() -> hooksSubscription.dispose())
        .thenVerify();

    Flux<String> output = webSocketFluxClient.get(Flux.empty());

    StepVerifier.create(output)
        .thenAwait(Duration.ofSeconds(5))      
        .thenCancel()
        .verify(Duration.ofSeconds(15));    

    assertThat(hooksList).containsExactly(
        "BEFORE_OPEN",
        "AFTER_OPEN",
        "AFTER_CLOSE"
    );

    verifierComplete.waitComplete(Duration.ofSeconds(30));
  }
}
