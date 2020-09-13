package com.trickl.flux.publishers;

import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;

@Log
@RequiredArgsConstructor
public class SwitchableProcessor<T> implements Publisher<T> {

  protected final Publisher<T> innerPublisher;

  protected final AtomicReference<FluxSink<T>> innerSinkRef;

  protected final Disposable subscription;

  /**
   * Create a switchable processor.
   */
  public static <T> SwitchableProcessor<T> create(Publisher<?> switchSignal) {
    return create(switchSignal, "switchableProcessor");
  }

  /**
   * Create a switchable processor.
   */
  public static <T> SwitchableProcessor<T> create(Publisher<?> switchSignal, String name) {
    return create(switchSignal, 0, name);
  }

  /**
   * Create a switchable processor.
   */
  public static <T> SwitchableProcessor<T> create(
      Publisher<?> switchSignal, int history, String name) {
    AtomicReference<FluxSink<T>> sinkRef = new AtomicReference<FluxSink<T>>();

    Flux<T> publisher = createSwitchablePublisherAndSink(switchSignal, sinkRef, name)
        .log(name).replay(history).refCount();
    Disposable subscription = publisher.subscribe();

    return new SwitchableProcessor<>(publisher, sinkRef, subscription);
  }

  public FluxSink<T> sink() {
    return innerSinkRef.get();
  }

  public void complete() {
    innerSinkRef.get().complete();
    subscription.dispose();
  }
  
  protected static <T> Flux<T> createSwitchablePublisherAndSink(
      Publisher<?> switchSignal, AtomicReference<FluxSink<T>> sinkRef, String name) {
    log.info("Creating switchableProcessor - " + name);
    UnicastProcessor<T> initialEmitter =  UnicastProcessor.<T>create();
    sinkRef.set(initialEmitter.sink());

    Flux<Publisher<T>> mergedPublishers = Flux.from(switchSignal)
        .cache(1).log("cachedSwitchSignal").map(signal -> {        
          log.info("switchableProcessor received signal - " + name);
          UnicastProcessor<T> nextEmitter =  UnicastProcessor.<T>create();
          sinkRef.get().complete();
          sinkRef.set(nextEmitter.sink());
          return nextEmitter;
        });

    return Flux.switchOnNext(Mono.just(initialEmitter).thenMany(mergedPublishers));
  }

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    innerPublisher.subscribe(subscriber);
  }
}