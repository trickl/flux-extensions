package com.trickl.flux.mappers;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
@Builder
public class ExpectedResponseTimeoutFactory<T> implements 
    BiFunction<Publisher<Duration>, Publisher<T>, Publisher<T>> {

  @Builder.Default
  private Predicate<T> isResponse = value -> true;

  @Builder.Default
  private boolean isRecurring = false;

  @Builder.Default
  private BiFunction<TimeoutException, Duration, ? extends Throwable> timeoutExceptionMapper
      = (error, duration) -> error;

  public Flux<T> apply(
      Publisher<Duration> frequencyPublisher, Publisher<T> stream) {
    return Flux.from(isRecurring ? Flux.from(frequencyPublisher) : Mono.from(frequencyPublisher))
        .switchMap(period -> timeoutNoElement(period, stream));    
  }

  protected Publisher<T> timeoutNoElement(
      Duration frequency,
      Publisher<T> stream) {
    if (frequency.isZero()) {
      log.info("Cancelling response expectation");
      return Mono.empty();
    }
    log.info("Expecting responses every " + frequency.toString());

    Flux<T> filteredStream = Flux.from(stream).filter(isResponse);
    Flux<T> timeout = isRecurring ? filteredStream.timeout(frequency) 
        : filteredStream.timeout(Mono.delay(frequency));

    return timeout
      .onErrorMap(
          error -> {
            if (error instanceof TimeoutException) {
              return timeoutExceptionMapper.apply((TimeoutException) error, frequency);
            }
            return error;
          })
      .ignoreElements();
  }
}
