package com.trickl.flux.publishers;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@RequiredArgsConstructor
public class FixedRateTimePublisher implements Supplier<Flux<Instant>> {

  private final Duration delay;
  private final Duration period;
  private final Supplier<Instant> timeSupplier;
  private final Scheduler scheduler;

  /**
   * Create a new flux that periodically emits a time from a time source.
   *
   * @param period The period between polls
   */
  public FixedRateTimePublisher(Duration period) {
    this(Duration.ZERO, period, Instant::now, Schedulers.parallel());
  }

  @Override
  public Flux<Instant> get() {
    return new FixedRatePublisher(delay, period, scheduler).get().map(val -> timeSupplier.get());
  }
}
