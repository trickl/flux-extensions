package com.trickl.flux.websocket.stomp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.config.WebSocketConfiguration;
import com.trickl.flux.websocket.BaseWebSocketClientTest;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Mono;

@RunWith(SpringRunner.class)
@ActiveProfiles({"unittest"})
@SpringBootTest(classes = WebSocketConfiguration.class)
public class StompFluxClientTest extends BaseWebSocketClientTest {

  @Autowired ObjectMapper objectMapper = new ObjectMapper();

  @BeforeEach
  private void setup() {
    startServer(objectMapper);
  }

  @AfterEach
  private void shutdown() throws IOException, InterruptedException {
    server.shutdown();
  }

  @Test
  public void testEchoWebSocket() throws IOException, InterruptedException {

    handleRequest()
        .awaitOpen()
        .thenSend("MESSAGE 1")
        .thenWait(500, TimeUnit.MILLISECONDS)
        .thenSend("MESSAGE 2")
        .thenWait(500, TimeUnit.MILLISECONDS)
        .thenSend("MESSAGE 3")
        .thenWait(500, TimeUnit.MILLISECONDS)
        .thenSend("MESSAGE 4")
        .thenWait(500, TimeUnit.MILLISECONDS)
        .thenSend("MESSAGE 5")
        .thenWait(500, TimeUnit.MILLISECONDS)
        .thenClose();

    WebSocketClient client = new ReactorNettyWebSocketClient();
    StompFluxClient stompClient =
        new StompFluxClient(
            client,
            URI.create("http://localhost/notused"),
            Mono.<HttpHeaders>empty(),
            objectMapper,
            Duration.ofSeconds(5),
            Duration.ofSeconds(5));

    stompClient.connect();
  }
}
