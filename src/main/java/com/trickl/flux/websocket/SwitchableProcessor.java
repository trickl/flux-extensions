package com.trickl.flux.websocket;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.UnicastProcessor;

@Log
@RequiredArgsConstructor
public class SwitchableProcessor<T> implements Publisher<T> {

  protected final Publisher<T> innerPublisher;

  protected final FluxSinkRef<T> innerSinkRef;

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
    FluxSinkRef<T> sinkRef = new FluxSinkRef<>();

    Flux<T> publisher = createSwitchablePublisherAndSink(switchSignal, sinkRef, name)
        .log(name).replay(history).refCount();
    Disposable subscription = publisher.subscribe();

    return new SwitchableProcessor<>(publisher, sinkRef, subscription);
  }

  public FluxSink<T> sink() {
    return innerSinkRef.getSink();
  }

  public void complete() {
    innerSinkRef.getSink().complete();
    subscription.dispose();
  }
  
  protected static <T> Flux<T> createSwitchablePublisherAndSink(
      Publisher<?> switchSignal, FluxSinkRef<T> sinkRef, String name) {
    log.info("Creating switchableProcessor - " + name);
    Flux<Publisher<T>> mergedPublishers = Flux.from(switchSignal)
        .cache(1).log("cachedSwitchSignal").map(signal -> {        
          log.info("switchableProcessor received signal - " + name);
          UnicastProcessor<T> nextEmitter =  UnicastProcessor.<T>create();
          if (sinkRef.getSink() != null) {
            sinkRef.getSink().complete();
          }
          sinkRef.setSink(nextEmitter.sink());
          return nextEmitter;
        });

    return Flux.switchOnNext(mergedPublishers);
  }

  @Data
  private static class FluxSinkRef<T> {
    protected FluxSink<T> sink;
  }

  @Override
  public void subscribe(Subscriber<? super T> subscriber) {
    innerPublisher.subscribe(subscriber);
  }
}