package com.trickl.flux.retry;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class ExponentialBackoffRetry implements Function<Flux<Throwable>, Flux<Long>> {
  final Duration initialRetryDelay;
  final int maxRetries;
  
  @Override
  public Flux<Long> apply(Flux<Throwable> errorFlux) {
    return errorFlux
        .scan(Collections.<Throwable>emptyList(), ExponentialBackoffRetry::accumulateErrors)
        .map(List::size).flatMap(errorCount -> {
          if (errorCount > maxRetries) {
            return Mono.error(new IllegalStateException("Max retries exceeded"));
          } else if (errorCount > 0) {
            Duration retryDelay = getExponentialRetryDelay(initialRetryDelay, errorCount);
            log.info("Will retry after error in " + retryDelay);
            return Mono.delay(retryDelay).doOnNext(x -> log.info("Retrying..."));
          }
          return Mono.empty();
        });
  }

  protected static List<Throwable> accumulateErrors(List<Throwable> last, Throwable latest) {
    return Stream.concat(last.stream(), Stream.of(latest)).collect(Collectors.toList());
  }

  private Duration getExponentialRetryDelay(Duration initial, int errorCount) {
    long initialMs = initial.toMillis();
    long exponentialMs = initialMs * (long) Math.pow(2, errorCount - 1.0);
    return Duration.ofMillis(exponentialMs);
  }
}