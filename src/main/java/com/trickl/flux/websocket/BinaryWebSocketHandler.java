package com.trickl.flux.websocket;

import com.trickl.flux.mappers.ThrowableMapper;
import java.io.IOException;
import java.io.InputStream;
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
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class BinaryWebSocketHandler implements WebSocketHandler {

  private final FluxSink<byte[]> receive;

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
               log.info("Output completed with signal - " + signalType.name()));
    
    return Mono.usingWhen(Mono.just(session), sessionResource -> 
      Mono.zip(input, output).then()
          .doFinally(signalType -> log.info(
               "Binary socket stream completed with signal - " + signalType.name())),
      sessionResource -> session.close().doOnSuccess(element -> 
      log.log(Level.INFO, "Closed session.")
    ));
  }

  protected WebSocketMessage createMessage(WebSocketSession session, byte[] message) {
    return session.binaryMessage(bufferFactory -> {
      DataBuffer payload = bufferFactory.allocateBuffer(message.length);
      payload.write(message);
      return payload;
    });
  }

  protected WebSocketMessage handleMessage(WebSocketMessage message) throws IOException {
    log.info("Handling message...");
    message.retain();
    DataBuffer payload = message.getPayload();
    InputStream payloadStream = payload.asInputStream();
    byte[] buffer = IOUtils.toByteArray(payloadStream);
    receive.next(buffer);
    return message;
  }
}
