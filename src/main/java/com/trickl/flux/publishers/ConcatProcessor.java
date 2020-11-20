package com.trickl.flux.publishers;

import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Log
@RequiredArgsConstructor
public class ConcatProcessor<T> implements Publisher<T> {

  protected final Publisher<T> innerPublisher;

  protected final AtomicReference<FluxSink<T>> innerSinkRef;

  protected final Publisher<Long> completeSignalPublisher;

  protected final FluxSink<Long> completeSignalSink;

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
   * @param <T> The type of flux element.
   * @param history How much history to replay on subscription.
   * @return A processor that can concatenate terminated streams.
   */
  public static <T> ConcatProcessor<T> create(int history) {
    AtomicReference<FluxSink<T>> sinkRef = new AtomicReference<FluxSink<T>>();
    EmitterProcessor<Long> completeSignalEmitter =  EmitterProcessor.<Long>create();
    FluxSink<Long> completeSignalSink = completeSignalEmitter.sink();

    Flux<T> publisher = createSwitchOnCompletePublisher(
        sinkRef, completeSignalEmitter, completeSignalSink)
        .replay(history).refCount();
    Disposable subscription = publisher.subscribe();

    return new ConcatProcessor<>(
        publisher, sinkRef, completeSignalEmitter, completeSignalSink, subscription);
  }

  /**
   * Get a sink for inserting signals.
   * 
   * @return A FluxSink.
   */
  public FluxSink<T> sink() {
    return innerSinkRef.get();
  }

  /**
   * Complete the processor.
   */
  public void complete() {
    subscription.dispose();
    completeSignalSink.complete();
    innerSinkRef.get().complete();    
  }
  
  protected static <T> Flux<T> createSwitchOnCompletePublisher(
      AtomicReference<FluxSink<T>> sinkRef, 
      Publisher<Long> completeSignalPublisher,
       FluxSink<Long> completeSignalSink) {
    EmitterProcessor<T> initialEmitter =  EmitterProcessor.<T>create();
    sinkRef.set(initialEmitter.sink());
    Publisher<T> initialPublisher = 
        signalCompletion(initialEmitter, completeSignalSink);

    Flux<Publisher<T>> mergedPublishers = Flux.from(completeSignalPublisher)
        .map(signal -> {        
          log.log(Level.FINE, "Switching on complete signal");
          EmitterProcessor<T> nextEmitter =  EmitterProcessor.<T>create();          
          sinkRef.set(nextEmitter.sink());
          return signalCompletion(nextEmitter, completeSignalSink);
        });

    return Flux.concat(Flux.just(initialPublisher).concatWith(mergedPublishers));
  }

  protected static <T> Flux<T> signalCompletion(
      Publisher<T> publisher, FluxSink<Long> completeSignalSink) {
    return Flux.from(publisher)
        .doOnComplete(() -> {
          completeSignalSink.next(1L);
        });
  }

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    innerPublisher.subscribe(subscriber);
  }
}