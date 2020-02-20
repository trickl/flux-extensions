package com.trickl.flux.publishers;

import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@RequiredArgsConstructor
public class FixedRatePublisher implements Supplier<Flux<Long>> {

  private final Duration delay;
  private final Duration period;
  private final Scheduler scheduler;

  /**
   * Create a new flux that emitters an incrementing long at a fixed rate.
   *
   * @param period the time period between emitted elements
   */
  public FixedRatePublisher(Duration period) {
    this(Duration.ZERO, period, Schedulers.parallel());
  }

  @Override
  public Flux<Long> get() {
    DirectProcessor<Long> processor = DirectProcessor.create();
    FluxSink<Long> sink = processor.sink();
    return processor.doOnSubscribe(sub -> onSubscribe(sink)).doOnCancel(() -> onCancel(sink));
  }

  private void onSubscribe(FluxSink<Long> sink) {
    try {
      AtomicLong count = new AtomicLong();
      Disposable emitterTask =
          scheduler.schedulePeriodically(
              () -> sink.next(count.getAndIncrement()),
              delay.toMillis(),
              period.toMillis(),
              TimeUnit.MILLISECONDS);
      sink.onCancel(emitterTask::dispose);
    } catch (RejectedExecutionException ree) {
      sink.error(ree);
    }
  }

  private void onCancel(FluxSink<Long> sink) {
    sink.complete();
  }
}
