package com.trickl.flux.publishers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class SubscriptionIdPublisherTest {

  @Test
  public void testSubscriptionIds() throws IOException {

    EmitterProcessor<Integer> processor = EmitterProcessor.create();    

    ArrayList<Integer> subscribed = new ArrayList<Integer>();
    ArrayList<Integer> unsubscribed = new ArrayList<Integer>();
    SubscriptionIdPublisher<Integer> subscriptionIdPublisher = 
        SubscriptionIdPublisher.<Integer>builder()
        .source(processor)
        .doOnSubscribe(id -> {          
          subscribed.add(id);
        })
        .doOnCancel(id -> {
          unsubscribed.add(id);
        })
        .build();
        
    AtomicReference<Disposable> subASubscription = new AtomicReference<>();
    AtomicReference<Disposable> subBSubscription = new AtomicReference<>();
    AtomicReference<Disposable> subCSubscription = new AtomicReference<>();

    Flux<Integer> flux = Flux.from(subscriptionIdPublisher);
        
    StepVerifier.create(flux)
        .then(() -> {
          assertArrayEquals(new Integer[] {1}, subscribed.toArray());
        })
        .then(() -> {        
          // No subscription - not connected  
          subASubscription.set(flux.subscribe());
        })
        .then(() -> {
          assertArrayEquals(new Integer[] {1, 2}, subscribed.toArray());
        })
        .then(() -> {
          // No subscription - not connected
          subBSubscription.set(flux.subscribe());
        })
        .then(() -> {
          assertArrayEquals(new Integer[] {1, 2, 3}, subscribed.toArray());
        })
        .then(() -> {
          // No subscription - not connected
          subCSubscription.set(flux.subscribe());
        })
        .then(() -> {
          assertArrayEquals(new Integer[] {1, 2, 3, 4}, subscribed.toArray());
        })        
        .then(() -> {
          assertArrayEquals(new Integer[] {}, unsubscribed.toArray());
        })
        .then(() -> {
          subASubscription.get().dispose();
        })
        .then(() -> {
          assertArrayEquals(new Integer[] {2}, unsubscribed.toArray());
        })
        .then(() -> {
          subBSubscription.get().dispose();
        })
        .then(() -> {
          assertArrayEquals(new Integer[] {2, 3}, unsubscribed.toArray());
        })
        .then(() -> {
          subCSubscription.get().dispose();
        })
        .then(() -> {
          assertArrayEquals(new Integer[] {2, 3, 4}, unsubscribed.toArray());
        })
        .then(() -> processor.sink().complete())
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }
}
