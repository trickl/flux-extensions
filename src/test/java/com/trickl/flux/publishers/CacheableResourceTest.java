package com.trickl.flux.publishers;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class CacheableResourceTest {

  @Test
  public void getCachedResource() throws IOException {

    CacheableResource<Integer> cachedResource = new CacheableResource<Integer>(
        last -> Mono.just(last == null ? 0 : last + 1), last -> false);

    StepVerifier.create(cachedResource.getResource())
        .expectNext(0)
        .expectComplete()
        .verify(Duration.ofSeconds(300));

    StepVerifier.create(cachedResource.getResource())
        .expectNext(0)
        .expectComplete()
        .verify(Duration.ofSeconds(300));

    StepVerifier.create(cachedResource.getResource())
        .expectNext(0)
        .expectComplete()
        .verify(Duration.ofSeconds(300));
  }

  @Test
  public void getCachedResourceSlowProvider() throws IOException {

    CacheableResource<Integer> cachedResource = new CacheableResource<Integer>(
        last -> Mono.just(last == null ? 0 : last + 1)
        .delayElement(Duration.ofMillis(500)), last -> false);

    AtomicReference<Disposable> secondSubscription = new AtomicReference<>();

    StepVerifier.create(cachedResource.getResource())
        .then(() -> secondSubscription.set(cachedResource.getResource().subscribe()))
        .expectNext(0)   
        .then(() -> secondSubscription.get().dispose())     
        .expectComplete()        
        .verify(Duration.ofSeconds(300));
  }
}
