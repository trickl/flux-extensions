package com.trickl.flux.websocket.stomp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.websocket.stomp.StompFrame;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class StompInputTransformer<T> implements Function<Publisher<byte[]>, Flux<StompFrame>> {

  private final ObjectMapper objectMapper;
  private final Class<T> messageType;

  @Override
  public Flux<StompFrame> apply(Publisher<byte[]> source) {
    StompMessageCodec<T> codec = new StompMessageCodec<>(objectMapper, messageType);
    return Flux.from(source)
        .flatMap(payload -> handleFrame(payload, codec));
  }

  protected Publisher<StompFrame> handleFrame(byte[] payload, StompMessageCodec<T> codec) {
    List<StompFrame> frames;
    try {
      frames = codec.decode(payload);
    } catch (IOException ex) {
      return Flux.error(ex);
    }
    
    return Flux.fromIterable(frames);
  }
}
