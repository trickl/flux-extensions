package com.trickl.flux.publishers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import java.time.Duration;
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
public class FixedRatePublisherTest {

  @Mock private Scheduler scheduler;

  @Mock private Disposable task;

  @Captor ArgumentCaptor<Runnable> taskCapture;

  private Subscription subscription;

  /** Setup the tests. */
  @BeforeEach
  public void init() {
    when(scheduler.schedulePeriodically(
            taskCapture.capture(), anyLong(), anyLong(), any(TimeUnit.class)))
        .thenReturn(task);
    subscription = null;
  }

  @Test
  public void testSubscribesCorrectly() {
    FixedRatePublisher publisher =
        new FixedRatePublisher(Duration.ZERO, Duration.ofSeconds(10), scheduler);
    Flux<Long> output = publisher.get();

    StepVerifier.create(output)
        .consumeSubscriptionWith(sub -> subscription = sub)
        .then(this::unsubscribe)
        .expectComplete()
        .verify();
  }

  @Test
  public void testGeneratesFirstCorrectly() {
    FixedRatePublisher publisher =
        new FixedRatePublisher(Duration.ZERO, Duration.ofSeconds(10), scheduler);
    Flux<Long> output = publisher.get();

    StepVerifier.create(output)
        .consumeSubscriptionWith(sub -> subscription = sub)
        .then(this::runScheduledTask)
        .expectNext(0L)
        .then(this::unsubscribe)
        .expectComplete()
        .verify();
  }

  @Test
  public void testGeneratesMultipleCorrectly() {
    FixedRatePublisher publisher =
        new FixedRatePublisher(Duration.ZERO, Duration.ofSeconds(10), scheduler);
    Flux<Long> output = publisher.get();

    StepVerifier.create(output)
        .consumeSubscriptionWith(sub -> subscription = sub)
        .then(this::runScheduledTask)
        .expectNext(0L)
        .then(this::runScheduledTask)
        .expectNext(1L)
        .then(this::runScheduledTask)
        .expectNext(2L)
        .then(this::runScheduledTask)
        .expectNext(3L)
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
