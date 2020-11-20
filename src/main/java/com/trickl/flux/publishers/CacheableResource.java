package com.trickl.flux.publishers;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class CacheableResource<T> {

  private final Function<T, Mono<T>> resourceGenerator;

  private final Predicate<T> shouldGenerate;

  private AtomicBoolean shouldGenerateOnNextRequest = new AtomicBoolean(true);

  private final DirectProcessor<T> lastResourceProcessor 
      = DirectProcessor.create();

  private final Flux<T> lastResourcePublisher 
      = lastResourceProcessor.cache(1);
  
  public void supplyOnNextRequest() {
    shouldGenerateOnNextRequest.set(true);
  }

  protected Mono<T> getResourceAndValidationKey() {            
    Mono<T> nextResource;    

    if (shouldGenerateOnNextRequest.getAndSet(false)) {
      log.info("Generating new resource");
      nextResource = resourceGenerator.apply(null);      
    } else {
      log.fine("Waiting on last resource");
      nextResource = Mono.<T, T>usingWhen(
        lastResourcePublisher, this::getNextResource,
        lastResource -> Mono.empty());
    } 

    return Mono.zip(lastResourcePublisher.next(), nextResource.doOnNext(resource -> {      
      log.info("Got resource - " + resource);
      lastResourceProcessor.sink().next(resource);
    }), (cachedResource, generatedResource) -> cachedResource);
  }

  protected Mono<T> getNextResource(T lastResource) {
    Mono<T> nextResource;
    if (shouldGenerate.test(lastResource)) {
      log.info("Creating new resource according to validation");
      nextResource = resourceGenerator.apply(lastResource);
    } else {
      nextResource = Mono.just(lastResource);
    }
    return nextResource;
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
