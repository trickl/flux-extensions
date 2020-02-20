package com.trickl.flux.websocket.stomp;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class StompInputTransformer implements Function<Publisher<byte[]>, Flux<StompFrame>> {

  @Override
  public Flux<StompFrame> apply(Publisher<byte[]> source) {
    StompMessageCodec codec = new StompMessageCodec();
    return Flux.from(source).flatMap(payload -> handleFrame(payload, codec));
  }

  protected Publisher<StompFrame> handleFrame(byte[] payload, StompMessageCodec codec) {
    List<StompFrame> frames;
    try {
      frames = codec.decode(payload);
    } catch (IOException ex) {
      return Flux.error(ex);
    }

    return Flux.fromIterable(frames);
  }
}
