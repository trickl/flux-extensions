package com.trickl.flux.transformers;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
@AllArgsConstructor
public class DifferentialTransformer<T, S> implements Function<Publisher<T>, Flux<S>> {

  private final BiFunction<T, T, S> transform;

  private T emptyValue = null;

  @Override
  public Flux<S> apply(Publisher<T> source) {
    return Flux.from(source)
        .map(Optional::of)
        .startWith(Optional.empty())
        .buffer(2, 1)
        .map(
            list ->
                transform.apply(
                    list.get(0).isPresent() ? list.get(0).get() : emptyValue,
                    list.size() > 1 && list.get(1).isPresent() ? list.get(1).get() : emptyValue));
  }
}
