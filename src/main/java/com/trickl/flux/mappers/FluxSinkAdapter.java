package com.trickl.flux.mappers;

import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.EmitResult;

@RequiredArgsConstructor
public class FluxSinkAdapter<T, S, E extends Exception> implements Sinks.Many<T> {

  private final Sinks.Many<S> inner;

  private final ThrowingFunction<T, Publisher<S>, E> mapper;

  @Override
  public Object scanUnsafe(Attr key) {
    return inner.scanUnsafe(key);
  }

  @Override
  public EmitResult tryEmitNext(T t) {
    try {
      Publisher<S> s = mapper.apply(t);
      Flux.from(s).doOnNext(value -> {
        inner.tryEmitNext(value);
      }).subscribe();      
    } catch (Exception throwable) {
      return inner.tryEmitError(throwable);
    }
    return EmitResult.OK;
  }

  @Override
  public EmitResult tryEmitComplete() {
    return inner.tryEmitComplete();
  }

  @Override
  public EmitResult tryEmitError(Throwable error) {
    return inner.tryEmitError(error);
  }

  @Override
  public void emitNext(T t, EmitFailureHandler failureHandler) {
    try {
      Publisher<S> s = mapper.apply(t);
      Flux.from(s).doOnNext(value -> {
        inner.emitNext(value, failureHandler);
      }).subscribe();      
    } catch (Exception throwable) {
      inner.emitError(throwable, failureHandler);
    }        
  }

  @Override
  public void emitComplete(EmitFailureHandler failureHandler) {
    inner.emitComplete(failureHandler);
  }

  @Override
  public void emitError(Throwable error, EmitFailureHandler failureHandler) {
    inner.emitError(error, failureHandler);
  }

  @Override
  public int currentSubscriberCount() {
    return inner.currentSubscriberCount();
  }

  @Override
  public Flux<T> asFlux() {
    return Flux.error(new RuntimeException("Adapter does not support output"));
  }   
}