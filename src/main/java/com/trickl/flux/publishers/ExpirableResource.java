package com.trickl.flux.publishers;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class ExpirableResource<T> {

  private final CacheableResource<T> cacheableResource;

  /**
   * Create a time limited cachable resource.
   * 
   * @param resourceGenerator The supplier of resources.
   * @param expiryAccessor Determine the expiration from the resource.
   * @param scheduler The flux scheduler.   
   */
  public ExpirableResource(
      Function<T, Mono<T>> resourceGenerator,
      Function<T, Instant> expiryAccessor,
      Scheduler scheduler) {
    cacheableResource = new CacheableResource<T>(
        resourceGenerator::apply,
        lastResource -> shouldGenerate(lastResource, expiryAccessor, scheduler)
    );
  }

  protected boolean shouldGenerate(
      T lastResource,
      Function<T, Instant> expiryAccessor,
      Scheduler scheduler) {
    Instant expiry = Optional.ofNullable(lastResource)
        .map(expiryAccessor)
        .orElse(Instant.ofEpochMilli(0));
    Instant now = Instant.ofEpochMilli(scheduler.now(TimeUnit.MILLISECONDS));

    if (now.isAfter(expiry)) {            
      return true;
    }

    return false;
  }


  /**
   * Force a new resource on the next request.
   */
  public void supplyOnNextRequest() {
    cacheableResource.supplyOnNextRequest();
  }

  /**
   * Get a new resource directly from the supplier.
   * 
   * @return A new resource.
   */
  public Mono<T> getResourceWithoutCache() {
    return cacheableResource.getResourceWithoutCache();
  }

  /**
   * Get a resource, making use of a cached value if one exists.
   *
   * @return A session Resource
   */
  public Mono<T> getResource() {   
    return cacheableResource.getResource();
  }
}
