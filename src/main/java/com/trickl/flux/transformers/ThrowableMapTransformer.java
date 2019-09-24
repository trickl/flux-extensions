package com.trickl.flux.transformers;

import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class ThrowableMapTransformer<T, S> implements Function<Publisher<T>, Flux<S>> {

  @FunctionalInterface
  public interface ThrowingFunction<T, S, E extends Exception> {
    S apply(T t) throws E;
  }

  private final ThrowingFunction<T, S, ?> mapper;

  /**
   * Get a flux of messages from the stream.
   *
   * @return A flux of (untyped) objects
   */
  public Flux<S> apply(Publisher<T> source) {
    return Flux.from(source).flatMap(this::map);
  }

  protected Publisher<S> map(T t) {
    try {
      return Mono.just(mapper.apply(t));
    } catch (Exception throwable) {
      return Flux.error(throwable);
    }
  }
}
