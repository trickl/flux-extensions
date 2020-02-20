package com.trickl.flux.publishers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
public class FixedRateTimePublisherTest {

  private static final Instant START_TIME = Instant.parse("2019-01-01T00:00:00Z");

  private class TimeSupplier {
    private Instant time = START_TIME;

    public Instant virtualTime() {
      Instant now = time;
      time = time.plus(Duration.ofDays(1));
      return now;
    }
  }

  @Mock private Scheduler scheduler;

  @Mock private Disposable task;

  @Captor ArgumentCaptor<Runnable> taskCapture;

  private Subscription subscription;

  private TimeSupplier timeSupplier;

  /** Setup the tests. */
  @BeforeEach
  public void init() {
    timeSupplier = new TimeSupplier();
    when(scheduler.schedulePeriodically(
            taskCapture.capture(), anyLong(), anyLong(), any(TimeUnit.class)))
        .thenReturn(task);
    subscription = null;
  }

  @Test
  public void testSubscribesCorrectly() {
    FixedRateTimePublisher publisher =
        new FixedRateTimePublisher(
            Duration.ZERO, Duration.ofSeconds(10), timeSupplier::virtualTime, scheduler);
    Flux<Instant> output = publisher.get();

    StepVerifier.create(output)
        .consumeSubscriptionWith(sub -> subscription = sub)
        .then(this::unsubscribe)
        .expectComplete()
        .verify();
  }

  @Test
  public void testGeneratesFirstCorrectly() {
    FixedRateTimePublisher publisher =
        new FixedRateTimePublisher(
            Duration.ZERO, Duration.ofSeconds(10), timeSupplier::virtualTime, scheduler);
    Flux<Instant> output = publisher.get();

    StepVerifier.create(output)
        .consumeSubscriptionWith(sub -> subscription = sub)
        .then(this::runScheduledTask)
        .expectNext(START_TIME)
        .then(this::unsubscribe)
        .expectComplete()
        .verify();
  }

  @Test
  public void testGeneratesMultipleCorrectly() {
    FixedRateTimePublisher publisher =
        new FixedRateTimePublisher(
            Duration.ZERO, Duration.ofSeconds(10), timeSupplier::virtualTime, scheduler);
    Flux<Instant> output = publisher.get();

    StepVerifier.create(output)
        .consumeSubscriptionWith(sub -> subscription = sub)
        .then(this::runScheduledTask)
        .expectNext(START_TIME)
        .then(this::runScheduledTask)
        .expectNext(START_TIME.plus(Duration.ofDays(1)))
        .then(this::runScheduledTask)
        .expectNext(START_TIME.plus(Duration.ofDays(2)))
        .then(this::runScheduledTask)
        .expectNext(START_TIME.plus(Duration.ofDays(3)))
        .then(this::unsubscribe)
        .expectComplete()
        .verify();
  }

  void unsubscribe() {
    if (subscription != null) {
      subscription.cancel();
    }
    subscription = null;
  }

  void runScheduledTask() {
    if (taskCapture.getValue() != null) {
      taskCapture.getValue().run();
    }
  }
}
