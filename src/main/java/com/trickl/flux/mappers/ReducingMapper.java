package com.trickl.flux.mappers;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
@AllArgsConstructor
public class ReducingMapper<T> implements Function<T, Publisher<? extends T>> {

  private final BinaryOperator<T> accumulator;

  private final T emptyValue;

  private AtomicReference<Optional<T>> lastValue = new AtomicReference<>(Optional.empty());

  @Override
  public Publisher<T> apply(T t) {
    return lastValue.accumulateAndGet(Optional.of(t), (optT, optLast) -> 
        Optional.of(accumulator.apply(optT.orElse(emptyValue), optLast.orElse(emptyValue))))
        .map(acc -> Mono.just(acc))
        .orElse(Mono.empty());
  }
}
