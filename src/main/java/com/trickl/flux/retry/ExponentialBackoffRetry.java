package com.trickl.flux.retry;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.retry.Retry;

@Log
@Builder
public class ExponentialBackoffRetry extends Retry {
  @Builder.Default private Duration initialRetryDelay = Duration.ofSeconds(1);
  @Builder.Default private Duration considerationPeriod = Duration.ofSeconds(32);
  @Builder.Default private int maxRetries = 3;
  @Builder.Default private String name = "default";
  @Builder.Default private Predicate<Throwable> shouldRetry = error -> true;
  
  @Override
  public Publisher<?> generateCompanion(Flux<Retry.RetrySignal> errorFlux) {
    return errorFlux
        .flatMap(retrySignal -> {
          if (!shouldRetry.test(retrySignal.failure())) {
            return Mono.<Retry.RetrySignal>error(
              new IllegalStateException("Failed retry predicate"));
          }
          return Mono.just(retrySignal.copy());
        })
        .elapsed()
        .scan(Collections.<Tuple2<Long, Retry.RetrySignal>>emptyList(),
            this::accumulateErrors)
        .map(List::size).flatMap(errorCount -> {
          if (errorCount > maxRetries) {
            return Mono.error(new IllegalStateException("Max retries exceeded"));
          } else if (errorCount > 0) {
            Duration retryDelay = getExponentialRetryDelay(initialRetryDelay, errorCount);
            log.info(name + " - will retry after error in " + retryDelay);
            return Mono.delay(retryDelay).doOnNext(x -> log.info(name + " - retrying..."));
          }
          return Mono.empty();
        });
  }

  protected List<Tuple2<Long, Retry.RetrySignal>> accumulateErrors(
      List<Tuple2<Long, Retry.RetrySignal>> last, Tuple2<Long, Retry.RetrySignal> latest) {
    long considerationStart = latest.getT1() - considerationPeriod.toMillis();
    return Stream.concat(last.stream(), Stream.of(latest))
      .filter(tuple -> tuple.getT1() > considerationStart)
      .collect(Collectors.toList());
  }

  private Duration getExponentialRetryDelay(Duration initial, int errorCount) {
    long initialMs = initial.toMillis();
    long exponentialMs = initialMs * (long) Math.pow(2, errorCount - 1.0);
    return Duration.ofMillis(exponentialMs);
  }
}