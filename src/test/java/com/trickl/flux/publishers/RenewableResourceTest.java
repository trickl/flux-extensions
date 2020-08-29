package com.trickl.flux.publishers;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class RenewableResourceTest {

  protected Mono<Tuple2<String, Instant>> createToken(Instant now) {
    Instant expiry = now.plus(Duration.ofSeconds(10));
    return Mono.just(Tuples.of("SESSION_TOKEN", expiry));
  }

  @Test
  public void getTokenAndExpiry() throws IOException {

    VirtualTimeScheduler scheduler = VirtualTimeScheduler.getOrSet();
    RenewableResource<Tuple2<String, Instant>> tokenSource = 
        new RenewableResource<Tuple2<String, Instant>>(
            () -> createToken(Instant.ofEpochMilli(scheduler.now(TimeUnit.MILLISECONDS))),
            (Tuple2<String, Instant> token) -> 
              createToken(Instant.ofEpochMilli(scheduler.now(TimeUnit.MILLISECONDS))),
            (Tuple2<String, Instant> token) -> token.getT2(),
        scheduler,
        Duration.ofSeconds(3));

    for (int i = 0; i < 35; ++i) {
      StepVerifier.create(tokenSource.getResource())
          .assertNext(
              token -> {
                assertThat(token.getT1()).isEqualTo("SESSION_TOKEN");
                assertThat(token.getT2()).isAfterOrEqualTo(
                    Instant.ofEpochMilli(scheduler.now(TimeUnit.MILLISECONDS)));
              })
          .expectComplete()
          .verify(Duration.ofSeconds(300));

      scheduler.advanceTimeBy(Duration.ofSeconds(1));
    }
  }
}
