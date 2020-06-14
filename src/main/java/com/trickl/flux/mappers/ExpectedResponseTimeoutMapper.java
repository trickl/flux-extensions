package com.trickl.flux.mappers;

import io.netty.handler.timeout.TimeoutException;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class ExpectedResponseTimeoutMapper<T> implements Function<T, Publisher<? extends T>> {

  private final Publisher<Duration> responseExpectationPublisher;

  private final Predicate<T> isResponse;

  private final Function<TimeoutException, ? extends Throwable> timeoutExceptionMapper;

  private final EmitterProcessor<T> responseProcessor = EmitterProcessor.create();

  @Override
  public Publisher<T> apply(T t) {
    if (isResponse.test(t)) {
      responseProcessor.sink().next(t);
    }

    return Flux.merge(Mono.just(t), 
      createResponseExpectation(responseExpectationPublisher, responseProcessor));
  }

  protected Publisher<T> createResponseExpectation(
      Publisher<Duration> responseExpectationPublisher, Publisher<T> stream) {
    return Flux.from(responseExpectationPublisher)
        .switchMap(period -> timeoutNoResponse(period, stream));    
  }

  protected Publisher<T> timeoutNoResponse(Duration frequency, Publisher<T> stream) {
    if (frequency.isZero()) {
      log.info("Cancelling response expectation");
      return Flux.empty();
    }
    log.info("Expecting responses every " + frequency.toString());

    return Flux.from(stream)
      .timeout(frequency)
      .onErrorMap(
          error -> {
            if (error instanceof TimeoutException) {
              return timeoutExceptionMapper.apply((TimeoutException) error);
            }
            return error;
          })
      .ignoreElements();
  }
}
