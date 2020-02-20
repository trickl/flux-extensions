package com.trickl.flux.mappers;

import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class ThrowableMapper<T, S> implements Function<T, Publisher<? extends S>> {

  @FunctionalInterface
  public interface ThrowingFunction<T, S, E extends Exception> {
    S apply(T t) throws E;
  }

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
