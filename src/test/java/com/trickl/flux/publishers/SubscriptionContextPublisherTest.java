package com.trickl.flux.publishers;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class SubscriptionContextPublisherTest {

  @Test
  public void testSubscriptionIds() throws IOException {

    EmitterProcessor<Integer> processor = EmitterProcessor.create();    

    AtomicInteger maxSubscriptionId = new AtomicInteger(0);
    ArrayList<Integer> unsubscribed = new ArrayList<Integer>();
    SubscriptionContextPublisher<Integer, Integer> subscriptionContextPublisher = 
        SubscriptionContextPublisher.<Integer, Integer>builder()
        .source(processor)
        .doOnSubscribe(() -> {
          return maxSubscriptionId.incrementAndGet();
        })
        .doOnCancel(id -> {
          unsubscribed.add(id);
        })
        .build();
        
    AtomicReference<Disposable> subASubscription = new AtomicReference<>();
    AtomicReference<Disposable> subBSubscription = new AtomicReference<>();
    AtomicReference<Disposable> subCSubscription = new AtomicReference<>();

    Flux<Integer> flux = Flux.from(subscriptionContextPublisher);
        
    StepVerifier.create(flux)
        .then(() -> {        
          // No subscription - not connected  
          subASubscription.set(flux.subscribe());
        })
        .then(() -> {
          // No subscription - not connected
          subBSubscription.set(flux.subscribe());
        })
        .then(() -> {
          // No subscription - not connected
          subCSubscription.set(flux.subscribe());
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
