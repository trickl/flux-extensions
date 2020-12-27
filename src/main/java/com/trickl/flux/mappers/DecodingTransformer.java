package com.trickl.flux.mappers;

import java.util.function.Function;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class DecodingTransformer<S, T> implements Function<Publisher<S>, Flux<T>> {

  private final ThrowingFunction<S, Publisher<T>, ? extends Exception>  decoder;

  @Override
  public Flux<T> apply(Publisher<S> source) {
    ThrowableMapper<S, Publisher<T>> mapper = 
        new ThrowableMapper<S, Publisher<T>>(decoder);
    return Flux.from(source).flatMap(bytes -> {
      return Flux.from(Flux.merge(mapper.apply(bytes)))
          .log("Decoding Transformer", Level.FINE);
    });
  }
}
