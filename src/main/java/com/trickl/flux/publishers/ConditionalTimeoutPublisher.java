package com.trickl.flux.publishers;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

@Log
@RequiredArgsConstructor
public class ConditionalTimeoutPublisher<T> implements Supplier<Mono<T>>  {

  private final Publisher<T> source;
  private final Duration timeout;
  private final Predicate<? super  T> condition;  
  private final Function<TimeoutException, Throwable> onTimeoutThrow;
  private final Runnable onTimeoutDo;
  private final Scheduler scheduler;

  @Override
  public Mono<T> get() {

    return Flux.from(source)
        .filter(condition)
        .timeout(timeout, scheduler)        
        .doOnError(error -> {
          if (error instanceof TimeoutException
              && onTimeoutDo != null) {
            onTimeoutDo.run();
          }
        })
        .onErrorContinue(error -> {
          if (error instanceof TimeoutException) {
            return onTimeoutThrow == null;
          }
          return false;
        }, (error, value) -> 
            log.log(Level.FINE, "Timeout captured.")
          )
        .onErrorMap(error -> {
          if (error instanceof TimeoutException 
              && onTimeoutThrow != null) {
            return onTimeoutThrow.apply((TimeoutException) error);
          } 
          return error;          
        })
        .ignoreElements();
  }
}