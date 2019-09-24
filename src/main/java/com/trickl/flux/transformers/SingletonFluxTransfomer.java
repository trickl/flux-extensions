package com.trickl.flux.transformers;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class SingletonFluxTransfomer<T> implements Function<Publisher<T>, Flux<T>> {

  private final AtomicReference<Flux<T>> sharedFlux = new AtomicReference<>();

  /**
   * Get a flux of messages from the stream.
   *
   * @return A flux of (untyped) objects
   */
  public Flux<T> apply(Publisher<T> source) {
    return sharedFlux.updateAndGet(flux -> {
      if (flux == null) {
        flux = Flux.from(source).publish().refCount();
      }
      return flux;
    });
  }
}
