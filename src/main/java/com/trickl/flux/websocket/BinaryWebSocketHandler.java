package com.trickl.flux.websocket;

import com.trickl.flux.transformers.ThrowableMapTransformer;

import java.io.IOException;
import java.io.InputStream;

import lombok.RequiredArgsConstructor;

import org.apache.commons.io.IOUtils;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class BinaryWebSocketHandler implements WebSocketHandler {

  private final FluxSink<byte[]> receive;

  private final Publisher<byte[]> send;

  @Override
  public Mono<Void> handle(WebSocketSession session) {
    ThrowableMapTransformer<WebSocketMessage, WebSocketMessage> messageHandler = 
        new ThrowableMapTransformer<>(this::handleMessage);
    Mono<Void> input = messageHandler.apply(session.receive()).then();

    Mono<Void> output =
        session.send(
            Flux.from(send)
                .map(message -> createMessage(session, message)));

    return Mono.zip(input, output)
        .doOnTerminate(receive::complete)
        .then(session.close());
  }

  protected WebSocketMessage createMessage(WebSocketSession session, byte[] message) {
    return session.binaryMessage(bufferFactory -> {
      DataBuffer payload = bufferFactory.allocateBuffer(message.length);
      payload.write(message);
      return payload;
    });
  }

  protected WebSocketMessage handleMessage(WebSocketMessage message) throws IOException {
    message.retain();
    DataBuffer payload = message.getPayload();
    InputStream payloadStream = payload.asInputStream();
    byte[] buffer = IOUtils.toByteArray(payloadStream);
    receive.next(buffer);
    return message;   
  }
}
