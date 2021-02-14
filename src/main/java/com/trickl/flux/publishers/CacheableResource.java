package com.trickl.flux.publishers;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import lombok.Builder;
import lombok.extern.java.Log;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
public class CacheableResource<T> {
  
  private final Function<T, Mono<T>> resourceGenerator;

  private final Predicate<T> shouldGenerate;

  private final Duration timeout;

  private final AtomicBoolean shouldGenerateOnNextRequest = new AtomicBoolean(true);

  /**
   * Create a cacheable resource.
   * @param resourceGenerator called to create a resource.
   * @param shouldGenerate (optional) - 
   *     test the last resource to see if a new should be created
   * @param timeout (optional) - maximum time for getting a cached resource.
   */
  @Builder
  public CacheableResource(
      Function<T, Mono<T>> resourceGenerator,
      Predicate<T> shouldGenerate,
      Duration timeout
  ) {
    this.resourceGenerator = resourceGenerator;
    this.shouldGenerate = Optional.ofNullable(shouldGenerate)
        .orElse(value -> false);
    this.timeout = Optional.ofNullable(timeout).orElse(Duration.ofMinutes(1));
  }

  private final DirectProcessor<T> lastResourceProcessor 
      = DirectProcessor.create();

  private final Flux<T> lastResourcePublisher 
      = lastResourceProcessor.cache(1)
      .log("lastResource", Level.FINER);
  
  public void supplyOnNextRequest() {
    shouldGenerateOnNextRequest.set(true);
  }

  protected synchronized Mono<T> getResourceAndValidationKey() {            
    Mono<T> nextResource;    

    if (shouldGenerateOnNextRequest.getAndSet(false)) {
      log.fine("Generating new resource");
      nextResource = resourceGenerator.apply(null);      
    } else {
      log.finer("Waiting on last resource");
      nextResource = Mono.<T, T>usingWhen(
        lastResourcePublisher, this::getNextResource,
        lastResource -> Mono.empty());
    } 

    return Mono.zip(lastResourcePublisher
        .next()
        .timeout(timeout)
        .log("cachedResource", Level.FINER), 
      nextResource.doOnNext(resource -> {      
        log.fine("Got resource - " + resource);
        lastResourceProcessor.sink().next(resource);
      }).log("generatedResource", Level.FINER),
      (cachedResource, generatedResource) -> generatedResource)
      .log("zipResource", Level.FINER);
  }

  protected Mono<T> getNextResource(T lastResource) {
    Mono<T> nextResource;
    if (shouldGenerate.test(lastResource)) {
      log.fine("Creating new resource according to validation");
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
