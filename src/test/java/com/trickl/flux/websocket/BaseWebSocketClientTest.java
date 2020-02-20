package com.trickl.flux.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.http.codec.json.Jackson2JsonDecoder;
import org.springframework.http.codec.json.Jackson2JsonEncoder;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.socket.client.StandardWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;

public abstract class BaseWebSocketClientTest {

  protected MockWebServer server;

  protected WebClient webClient;

  protected WebSocketClient webSocketClient;

  protected Validator validator;

  protected Supplier<HttpHeaders> webSocketHeaders;

  protected ScheduledExecutorService scheduledExecutorService;

  protected void startServer(ObjectMapper objectMapper) {
    server = new MockWebServer();

    ExchangeStrategies exchangeStrategies =
        ExchangeStrategies.builder()
            .codecs(
                config -> {
                  config
                      .defaultCodecs()
                      .jackson2JsonEncoder(
                          new Jackson2JsonEncoder(objectMapper, MediaType.APPLICATION_JSON));
                  config
                      .defaultCodecs()
                      .jackson2JsonDecoder(
                          new Jackson2JsonDecoder(objectMapper, MediaType.APPLICATION_JSON));
                })
            .build();

    webClient =
        WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector())
            .exchangeStrategies(exchangeStrategies)
            .baseUrl(server.url("/").toString())
            .build();

    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    validator = factory.getValidator();

    webSocketClient = new StandardWebSocketClient();

    webSocketHeaders = HttpHeaders::new;
  }

  protected String getTransportPath() {
    return server.url("/").uri().getPath();
  }

  protected MockWebSocket handleRequest() {
    MockWebSocket webSocket = new MockWebSocket();
    MockResponse response = new MockResponse().withWebSocketUpgrade(webSocket);
    this.server.enqueue(response);
    return webSocket;
  }
}
