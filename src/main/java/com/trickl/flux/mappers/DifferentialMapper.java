package com.trickl.flux.mappers;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;

@RequiredArgsConstructor
@AllArgsConstructor
public class DifferentialMapper<T, S> implements Function<T, Publisher<? extends S>> {

  private final BiFunction<T, T, Publisher<S>> transform;

  private final T emptyValue;

  private AtomicReference<Optional<T>> lastValue = new AtomicReference<>(Optional.empty());

  @Override
  public Publisher<S> apply(T t) {
    return transform.apply(lastValue.getAndSet(Optional.of(t)).orElse(emptyValue), t);
  }
}
