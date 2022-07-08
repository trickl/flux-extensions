package com.trickl.flux.publishers;

import static org.mockito.ArgumentMatchers.any;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class CacheableResourceTest {

  @Test
  public void getCachedResource() throws IOException {

    Function<Integer, Mono<Integer>> generator = Mockito.mock(Function.class);
    Mockito.when(generator.apply(any())).thenAnswer(invocation -> {
      Integer last = (Integer) invocation.getArguments()[0];
      return Mono.just(last == null ? 0 : last + 1);
    });

    CacheableResource<Integer> cachedResource = CacheableResource.<Integer>builder()
        .resourceGenerator(generator)
        .build();

    StepVerifier.create(cachedResource.getResource()).expectNext(0).expectComplete()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(cachedResource.getResource()).expectNext(0).expectComplete()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(cachedResource.getResource()).expectNext(0).expectComplete()
        .verify(Duration.ofSeconds(5));

    Mockito.verify(generator, Mockito.times(1)).apply(any());
  }

  @Test
  public void getCachedResourceWithoutCache() throws IOException {

    Function<Integer, Mono<Integer>> generator = Mockito.mock(Function.class);
    Mockito.when(generator.apply(any())).thenAnswer(invocation -> {
      Integer last = (Integer) invocation.getArguments()[0];
      return Mono.just(last == null ? 0 : last + 1);
    });

    CacheableResource<Integer> cachedResource = CacheableResource.<Integer>builder()
        .resourceGenerator(generator)
        .shouldGenerate(last -> true)
        .build();

    StepVerifier.create(cachedResource.getResource())
        .expectNext(0)
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(cachedResource.getResource())
        .expectNext(1)
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    StepVerifier.create(cachedResource.getResource())
        .expectNext(2)
        .expectComplete()
        .verify(Duration.ofSeconds(5));

    
    Mockito.verify(generator, Mockito.times(3)).apply(any());
  }

  @Test
  public void getCachedResourceSlowProvider() throws IOException {
    
    Function<Integer, Mono<Integer>> generator = Mockito.mock(Function.class);
    Mockito.when(generator.apply(any())).thenAnswer(invocation -> {
      Integer last = (Integer) invocation.getArguments()[0];
      return Mono.just(last == null ? 0 : last + 1)
          .delayElement(Duration.ofMillis(500));
    });

    CacheableResource<Integer> cachedResource = CacheableResource.<Integer>builder()
        .resourceGenerator(generator)
        .build();

    AtomicReference<Disposable> secondSubscription = new AtomicReference<>();

    StepVerifier.create(cachedResource.getResource())
        .then(() -> secondSubscription.set(cachedResource.getResource().subscribe()))
        .expectNext(0)   
        .then(() -> secondSubscription.get().dispose())     
        .expectComplete()        
        .verify(Duration.ofSeconds(5));
  }
}
