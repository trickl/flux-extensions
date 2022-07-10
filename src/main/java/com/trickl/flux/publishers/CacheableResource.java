package com.trickl.flux.publishers;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.extern.java.Log;
import reactor.core.publisher.Mono;

@Log
public class CacheableResource<T> {

  private final Function<T, Mono<T>> resourceGenerator;

  private final Predicate<T> shouldGenerate;

  private final AtomicBoolean shouldGenerateOnNextRequest = new AtomicBoolean(true);

  private final AsyncCache<Long, T> monoCache =
      Caffeine.newBuilder().maximumSize(1).buildAsync();

  private final Long cacheKey = 0L;

  /**
   * Create a cacheable resource.
   *
   * @param resourceGenerator called to create a resource.
   * @param shouldGenerate (optional) - test the last resource to see if a new should be created
   * @param timeout (optional) - maximum time for getting a cached resource.
   */
  @Builder
  public CacheableResource(
      Function<T, Mono<T>> resourceGenerator, Predicate<T> shouldGenerate, Duration timeout) {
    this.resourceGenerator = resourceGenerator;
    this.shouldGenerate = Optional.ofNullable(shouldGenerate).orElse(value -> false);
  }

  public void supplyOnNextRequest() {
    shouldGenerateOnNextRequest.set(true);
  }

  protected synchronized Mono<T> getResourceAndValidationKey() {
    Mono<T> nextResource;

    if (shouldGenerateOnNextRequest.getAndSet(false)) {
      nextResource = resourceGenerator.apply(null);
    } else {
      CompletableFuture<T> fromCache = monoCache.getIfPresent(cacheKey);
      if (fromCache != null) {
        nextResource = Mono.fromFuture(fromCache).flatMap(last -> getNextResource(last));
      } else {
        nextResource = resourceGenerator.apply(null);
      }
    }

    monoCache.put(cacheKey, nextResource.toFuture());
    return Optional.ofNullable(monoCache.getIfPresent(cacheKey))
        .map(Mono::fromFuture)
        .orElse(Mono.empty());
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
