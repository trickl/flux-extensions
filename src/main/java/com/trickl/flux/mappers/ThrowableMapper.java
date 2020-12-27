package com.trickl.flux.mappers;

import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class ThrowableMapper<T, S> implements Function<T, Publisher<? extends S>> {

  private final ThrowingFunction<T, S, ?> mapper;

  @Override
  public Publisher<S> apply(T t) {
    try {
      return Mono.just(mapper.apply(t));
    } catch (Exception throwable) {
      return Flux.error(throwable);
    }
  }
}
