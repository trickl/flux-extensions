package com.trickl.flux.mappers;

import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class DecodingTransformer<S, T> implements Function<Publisher<S>, Flux<T>> {

  private final ThrowingFunction<S, List<T>, ? extends Exception>  decoder;

  @Override
  public Flux<T> apply(Publisher<S> source) {
    ThrowableMapper<S, List<T>> mapper = 
        new ThrowableMapper<S, List<T>>(decoder);
    return Flux.from(source).flatMap(bytes -> {
      return Flux.from(mapper.apply(bytes))
      .flatMap(list -> Flux.fromStream(list.stream()))
          .log("Decoding Transformer", Level.FINE);
    });
  }
}
