package com.trickl.flux.mappers;

import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DelayCancel {

  /**
   * Delay propagating a cancel signal upstream until the supplied mono completes.
   * @param <T> The type of source flux
   * @param source The source publisher to act upon
   * @param onCancel The action to perform prior to sending the cancel
   * @return
   */
  public static <T> Publisher<T> apply(Publisher<T> source, Mono<Void> onCancel) {
    AtomicReference<Disposable> subscriptionRef = new AtomicReference<>();
        
    return Flux.from(source)
      .publish()
      .autoConnect(1, subscriptionRef::set)
      .doOnCancel(() -> {
        onCancel.then(Mono.fromRunnable(
          () -> subscriptionRef.get().dispose())).subscribe();
      });
  }
}