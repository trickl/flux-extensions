package com.trickl.flux.mappers;

import java.util.List;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class EncodingTransformer<T, S> implements Function<Publisher<T>, Flux<S>> {

  private final ThrowingFunction<T, List<S>, ? extends Exception> encoder;

  @Override
  public Flux<S> apply(Publisher<T> source) {
    ThrowableMapper<T, List<S>> mapper = 
        new ThrowableMapper<T, List<S>>(encoder);
  
    return Flux.from(source)
      .filter(frame -> frame != null)
      .<S>flatMap(t -> Flux.from(mapper.apply(t))
      .flatMap(list -> Flux.fromStream(list.stream())));
  }
}