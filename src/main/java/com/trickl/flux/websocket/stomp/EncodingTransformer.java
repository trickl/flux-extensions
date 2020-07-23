package com.trickl.flux.websocket.stomp;

import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.mappers.ThrowingFunction;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class EncodingTransformer<T> implements Function<Publisher<T>, Flux<byte[]>> {

  private final ThrowingFunction<T, byte[], ? extends Exception> encoder;

  @Override
  public Flux<byte[]> apply(Publisher<T> source) {
    ThrowableMapper<T, byte[]> mapper = 
        new ThrowableMapper<T, byte[]>(encoder);
    return Flux.from(source).flatMap(frame -> {
      return frame == null ? Mono.empty() : mapper.apply(frame);
    });
  }
}