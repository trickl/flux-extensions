package com.trickl.flux.publishers;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class RenewableResource<T> {

  private final ExpirableResource<T> expiryableResource;

  /**
   * Create a time limited cachable resource.
   * 
   * @param resourceGenerator The supplier of resources.
   * @param resourceRenewer Take a resource and make it usable again.
   * @param expiryAccessor Determine the expiration from the resource.
   * @param scheduler The flux scheduler.  
   * @param resourceRenewPeriodBeforeExpiry When should we renew?
   */
  public RenewableResource(
      Supplier<Mono<T>> resourceGenerator,
      Function<T, Mono<T>> resourceRenewer,
      Function<T, Instant> expiryAccessor,
      Scheduler scheduler,
      Duration resourceRenewPeriodBeforeExpiry) {
    expiryableResource = new ExpirableResource<T>(
        resource -> generate(
          resource, 
          resourceGenerator, 
          resourceRenewer,
          expiryAccessor,
          scheduler),
        resource -> getRenewTime(resource, expiryAccessor, resourceRenewPeriodBeforeExpiry),
        scheduler);
  }

  protected Mono<T> generate(
      T lastResource,
      Supplier<Mono<T>> resourceGenerator,
      Function<T, Mono<T>> resourceRenewer,
      Function<T, Instant> expiryAccessor,
      Scheduler scheduler) {
    Instant now = Instant.ofEpochMilli(scheduler.now(TimeUnit.MILLISECONDS));
    Instant expiry = Optional.ofNullable(lastResource)
        .map(expiryAccessor)
        .orElse(Instant.ofEpochMilli(0));
    if (lastResource == null || now.isAfter(expiry)) {
      return resourceGenerator.get();
    }

    return resourceRenewer.apply(lastResource);
  }

  protected Instant getRenewTime(
      T resource, 
      Function<T, Instant> expiryAccessor,
      Duration resourceRenewPeriodBeforeExpiry) {
    return Optional.ofNullable(resource)
        .map(expiryAccessor)
        .map(expiry -> expiry.minus(resourceRenewPeriodBeforeExpiry))
        .orElse(Instant.ofEpochMilli(0));
  }

  /**
   * Force a new resource on the next request.
   */
  public void supplyOnNextRequest() {
    expiryableResource.supplyOnNextRequest();
  }

  /**
   * Get a new resource directly from the supplier.
   * 
   * @return A new Resource
   */
  public Mono<T> getResourceWithoutCache() {
    return expiryableResource.getResourceWithoutCache();
  }

  /**
   * Get a resource, making use of a cached value if one exists.
   *
   * @return A session Resource
   */
  public Mono<T> getResource() {   
    return expiryableResource.getResource();
  }
}
