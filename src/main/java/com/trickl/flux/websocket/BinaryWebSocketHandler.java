package com.trickl.flux.websocket;

import com.trickl.flux.mappers.ThrowableMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.apache.commons.io.IOUtils;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class BinaryWebSocketHandler implements WebSocketHandler {

  private final Consumer<byte[]> receive;

  private final Publisher<byte[]> send;

  @Override
  public Mono<Void> handle(WebSocketSession session) {
    Mono<Void> input = session.receive()
        .log("receive", Level.FINER)
        .flatMap(new ThrowableMapper<>(this::handleMessage)).then()
        .doFinally(signalType -> log.info("Input completed with signal - " + signalType.name()));

    Mono<Void> output =
        session.send(Flux.from(send)
           .log("send", Level.FINER)
           .map(message -> createMessage(session, message)))
           .doFinally(signalType -> 
               log.info("Output completed with signal - " 
               + signalType.name())).log("output", Level.FINER);
    
    
    return Mono.zip(input, output).then()
        .then(Mono.fromRunnable(() -> log.info("BinaryWebSocketHandler complete")));
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
    receive.accept(buffer);
    return message;
  }
}
