package com.trickl.flux.publishers;

import com.trickl.flux.transformers.DifferentialTransformer;

import java.time.Instant;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class DifferentialPollPublisher<T> implements Supplier<Flux<T>> {
  
  private final Publisher<Instant> timePublisher;
  private final BiFunction<Instant, Instant, Publisher<T>> pollBetween;

  @Override
  public Flux<T> get() {
    DifferentialTransformer<Instant, Publisher<T>> pollTransform =
        new DifferentialTransformer<>(pollBetween);

    return pollTransform.apply(timePublisher)         
         .flatMap(pub -> pub);
  }
}