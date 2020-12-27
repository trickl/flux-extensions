package com.trickl.flux.mappers;

import java.util.function.LongConsumer;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.util.context.Context;

@RequiredArgsConstructor
public class FluxSinkAdapter<T, S, E extends Exception> implements FluxSink<T> {

  private final FluxSink<S> inner;

  private final ThrowingFunction<T, Publisher<S>, E> mapper;

  @Override
  public void complete() {
    inner.complete();
  }

  @Override
  public Context currentContext() {
    return inner.currentContext();
  }

  @Override
  public void error(Throwable e) {
    inner.error(e);
  }

  @Override
  public FluxSink<T> next(T t) {
    try {
      Publisher<S> s = mapper.apply(t);
      Flux.from(s).doOnNext(value -> {
        inner.next(value);
      }).subscribe();      
    } catch (Exception throwable) {
      inner.error(throwable);
    }    
    return this;
  }

  @Override
  public long requestedFromDownstream() {
    return inner.requestedFromDownstream();
  }

  @Override
  public boolean isCancelled() {
    return inner.isCancelled();
  }

  @Override
  public FluxSink<T> onRequest(LongConsumer consumer) {
    inner.onRequest(consumer);
    return this;
  }

  @Override
  public FluxSink<T> onCancel(Disposable d) {
    inner.onCancel(d);
    return this;
  }

  @Override
  public FluxSink<T> onDispose(Disposable d) {
    inner.onDispose(d);
    return this;
  }   
}