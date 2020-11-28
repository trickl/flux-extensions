package com.trickl.flux.publishers;

import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Builder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;

@Builder
public class SubscriptionContextPublisher<T, ContextT> implements Publisher<T> {

  private Publisher<T> source;

  private Supplier<ContextT> doOnSubscribe;

  private Consumer<ContextT> doOnCancel;

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    ContextT context = doOnSubscribe != null ? doOnSubscribe.get() : null;      
    Flux.from(source)
        .doOnCancel(() -> {
          if (doOnCancel != null) {
            doOnCancel.accept(context);
          }
        })
        .subscribe(subscriber);
  }
}