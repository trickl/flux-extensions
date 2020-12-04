package com.trickl.flux.publishers;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.Builder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public class SubscriptionIdPublisher<T> implements Publisher<T> {

  private final SubscriptionContextPublisher<T, Integer> contextPublisher;

  private final AtomicInteger maxSubscriptionId = new AtomicInteger(0);

  /**
   * Create a publisher with a consistent id on subscribe and cancel.
   * @param source The underlying source
   * @param doOnSubscribe the subscribe handler
   * @param doOnCancel the cancel handler
   * @param doOnTerminate the terminate handler
   */
  @Builder
  public SubscriptionIdPublisher(
      Publisher<T> source,
      Consumer<Integer> doOnSubscribe,
      Consumer<Integer> doOnCancel,
      Consumer<Integer> doOnTerminate) {
    contextPublisher = SubscriptionContextPublisher.<T, Integer>builder()
      .source(source)
      .doOnSubscribe(() -> {
        Integer id = maxSubscriptionId.incrementAndGet();
        if (doOnSubscribe != null) {
          doOnSubscribe.accept(id);
        }
        return id;
      })
      .doOnCancel(id -> {
        if (doOnCancel != null) {
          doOnCancel.accept(id);
        }
      })
      .doOnTerminate(id -> {
        if (doOnTerminate != null) {
          doOnTerminate.accept(id);
        }
      })
      .build();
  }

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    contextPublisher.subscribe(subscriber);
  }
}