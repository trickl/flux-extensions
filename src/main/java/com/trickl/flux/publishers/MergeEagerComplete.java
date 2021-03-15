package com.trickl.flux.publishers;

import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;

@RequiredArgsConstructor
public class MergeEagerComplete<T> implements Publisher<T> {
  private final Publisher<T> first;
  private final Publisher<T> second;

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    Flux.from(second).doOnComplete(subscriber::onComplete);
    Flux.merge(
        Flux.from(first).doOnComplete(subscriber::onComplete),
        Flux.from(second).doOnComplete(subscriber::onComplete))
        .subscribeWith(subscriber);
  }
}
