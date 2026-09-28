package com.trickl.flux.routing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

public class MapRouterTest {

  @Test
  public void testSameDestinationSharesFlux() {
    MapRouter<String, String> router = MapRouter.<String, String>builder().build();
    Flux<String> source = Flux.just("a", "b");

    assertThat(router.route(source, "topic")).isSameAs(router.route(source, "topic"));
    assertThat(router.route(source, "topic")).isNotSameAs(router.route(source, "other"));
  }

  @Test
  public void testRoutesWhileCreatingAnotherRoute() {
    AtomicReference<MapRouter<String, String>> routerReference = new AtomicReference<>();
    MapRouter<String, String> router =
        MapRouter.<String, String>builder()
            .fluxCreator(
                (source, destination) -> {
                  // Creating one route can depend on another
                  if (destination.startsWith("child")) {
                    routerReference.get().route(source, "parent-of-" + destination);
                  }
                  return Flux.from(source);
                })
            .build();
    routerReference.set(router);

    Flux<String> source = Flux.just("a");
    assertThat(router.route(source, "child-1").collectList().block()).containsExactly("a");
  }

  @Test
  public void testConcurrentRoutes() throws Exception {
    MapRouter<Integer, Integer> router = MapRouter.<Integer, Integer>builder().build();
    Flux<Integer> source = Flux.range(0, 3);
    int threads = 16;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<List<Integer>>> results =
          IntStream.range(0, threads * 20)
              .mapToObj(
                  i ->
                      executor.submit(
                          () -> {
                            start.await();
                            return router.route(source, i % 40).collectList().block();
                          }))
              .collect(Collectors.toList());
      start.countDown();

      for (Future<List<Integer>> result : results) {
        assertThat(result.get(30, TimeUnit.SECONDS)).isNotNull();
      }
    } finally {
      executor.shutdownNow();
    }
  }
}
