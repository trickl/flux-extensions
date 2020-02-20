package com.trickl.flux.websocket.stomp;

import java.io.IOException;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class StompOutputTransformer implements Function<Publisher<StompFrame>, Flux<byte[]>> {

  @Override
  public Flux<byte[]> apply(Publisher<StompFrame> source) {
    StompMessageCodec codec = new StompMessageCodec();
    return Flux.from(source).flatMap(payload -> sendFrame(payload, codec));
  }

  protected Publisher<byte[]> sendFrame(StompFrame payload, StompMessageCodec codec) {

    if (payload == null) {
      return Mono.empty();
    }

    byte[] encoded;
    try {
      encoded = codec.encode(payload);
    } catch (IOException ex) {
      return Mono.error(ex);
    }

    return Mono.just(encoded);
  }
}
