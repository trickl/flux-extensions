package com.trickl.flux.publishers;

import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@Log
@RequiredArgsConstructor
public class ConcatProcessor<T> implements Publisher<T> {

  protected final Publisher<T> innerPublisher;

  protected final AtomicReference<Sinks.Many<T>> innerSinkRef;

  protected final Sinks.Many<Long> completeSignalSink;

  protected final Disposable subscription;

  /**
   * Create a processor that can concatenate terminated streams.
   * 
   * @param <T> The type of flux element.
   * @return A processor that can concatenate terminated streams.
   */
  public static <T> ConcatProcessor<T> create() {
    return create(0);
  }

  /**
   * Create a processor that can concatenate terminated streams.
   * 
   * @param <T>     The type of flux element.
   * @param history How much history to replay on subscription.
   * @return A processor that can concatenate terminated streams.
   */
  public static <T> ConcatProcessor<T> create(int history) {
    return create(0, Mono.empty());
  }

  /**
   * Create a processor that can concatenate terminated streams.
   * 
   * @param <T>             The type of flux element.
   * @param history         How much history to replay on subscription.
   * @param historySupplier Initial supplier for history
   * @return A processor that can concatenate terminated streams.
   */
  public static <T> ConcatProcessor<T> create(int history, Publisher<T> historySupplier) {
    AtomicReference<Sinks.Many<T>> sinkRef = new AtomicReference<Sinks.Many<T>>();
    Sinks.Many<Long> completeSignalSink = Sinks.many().multicast().onBackpressureBuffer();

    Flux<T> publisher = Flux.from(historySupplier).thenMany(createSwitchOnCompletePublisher(
        sinkRef, completeSignalSink))
        .replay(history).refCount();
    Disposable subscription = publisher.subscribe();

    return new ConcatProcessor<>(
        publisher, sinkRef, completeSignalSink, subscription);
  }

  /**
   * Get a sink for inserting signals.
   * 
   * @return A FluxSink.
   */
  public Sinks.Many<T> sink() {
    return innerSinkRef.get();
  }

  /**
   * Complete the processor.
   */
  public void complete() {
    subscription.dispose();
    completeSignalSink.tryEmitComplete();
    innerSinkRef.get().tryEmitComplete();
  }

  protected static <T> Flux<T> createSwitchOnCompletePublisher(
      AtomicReference<Sinks.Many<T>> sinkRef,
      Sinks.Many<Long> completeSignalSink) {
    Sinks.Many<T> initialSink = Sinks.many().multicast().onBackpressureBuffer();
    sinkRef.set(initialSink);
    Publisher<T> initialPublisher = signalCompletion(initialSink.asFlux(), completeSignalSink);

    Flux<Publisher<T>> mergedPublishers = completeSignalSink.asFlux()
        .map(signal -> {
          log.log(Level.FINE, "Switching on complete signal");
          Sinks.Many<T> nextSink = Sinks.many().multicast().onBackpressureBuffer();
          sinkRef.set(nextSink);
          return signalCompletion(nextSink.asFlux(), completeSignalSink);
        });

    return Flux.concat(Flux.just(initialPublisher).concatWith(mergedPublishers));
  }

  protected static <T> Flux<T> signalCompletion(
      Publisher<T> publisher, Sinks.Many<Long> completeSignalSink) {
    return Flux.from(publisher)
        .doOnComplete(() -> {
          completeSignalSink.tryEmitNext(1L);
        });
  }

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    innerPublisher.subscribe(subscriber);
  }
}