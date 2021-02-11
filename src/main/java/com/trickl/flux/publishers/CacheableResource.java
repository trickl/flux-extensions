package com.trickl.flux.publishers;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class CacheableResource<T> {

  private final Function<T, Mono<T>> resourceGenerator;

  private final Predicate<T> shouldGenerate;

  private AtomicReference<Mono<T>> lastResourceRef = new AtomicReference<>();

  public void supplyOnNextRequest() {
    lastResourceRef.set(null);
  }

  protected Mono<T> getResourceAndValidationKey() {   
    return lastResourceRef.updateAndGet(lastResource -> {
      if (lastResource == null) {
        return resourceGenerator.apply(null).cache();
      } else {
        return lastResource.flatMap(last -> {
          if (shouldGenerate.test(last)) {
            return resourceGenerator.apply(null).cache();
          } else {
            return Mono.just(last);
          }
        });
      }
    });
  }

  /**
   * Get a new resource directly from the supplier.
   * 
   * @return A session resource
   */
  public Mono<T> getResourceWithoutCache() {
    return resourceGenerator.apply(null);
  }

  /**
   * Get a resource, making use of a cached value if one exists.
   *
   * @return A session Resource
   */
  public Mono<T> getResource() {
    return getResourceAndValidationKey();      
  }
}
