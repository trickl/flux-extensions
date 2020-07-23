package com.trickl.flux.websocket.stomp;

import com.trickl.flux.mappers.ThrowableMapper;
import com.trickl.flux.mappers.ThrowingFunction;
import java.util.List;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class DecodingTransformer<T> implements Function<Publisher<byte[]>, Publisher<T>> {

  private final ThrowingFunction<byte[], List<T>, ? extends Exception>  decoder;

  @Override
  public Flux<T> apply(Publisher<byte[]> source) {
    ThrowableMapper<byte[], List<T>> mapper = 
        new ThrowableMapper<byte[], List<T>>(decoder);
    return Flux.from(source).flatMap(bytes -> {
      Publisher<List<T>> out = mapper.apply(bytes);
      return Flux.from(out).flatMap(Flux::fromIterable);
    });
  }
}
