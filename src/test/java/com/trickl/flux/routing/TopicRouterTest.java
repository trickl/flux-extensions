package com.trickl.flux.routing;

import com.trickl.flux.config.WebSocketConfiguration;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.java.Log;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.test.StepVerifier;

@Log
@ActiveProfiles({"unittest"})
@TestPropertySource(properties = { "spring.config.location=classpath:application.yml" })
@SpringBootTest(classes = WebSocketConfiguration.class)
public class TopicRouterTest {

  @Test
  public void testTopicFilters() {

    Flux<Integer> source = Flux.just(1, 2, 3, 4, 5);

    TopicRouter<Integer> topicRouter = TopicRouter.<Integer>builder()
        .topicFilter(topic -> "/evens".equals(topic)
            ? value -> value % 2 == 0
            : value -> value % 2 == 1)
        .build();

    Flux<Integer> evensTopic = topicRouter.route(source, "/evens");
    Flux<Integer> oddsTopic = topicRouter.route(source, "/odds");
    
    StepVerifier.create(evensTopic)
        .expectNext(2)       
        .expectNext(4)
        .expectComplete()
        .verify(Duration.ofSeconds(30));

    StepVerifier.create(oddsTopic)
        .expectNext(1)       
        .expectNext(3)
        .expectNext(5)
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }

  @Test
  public void testSubscribeIfAlreadyConnected() {
    Flux<Integer> source = Flux.just(1, 2, 3);

    TopicRouter<Integer> topicRouter = TopicRouter.<Integer>builder()
        .subscriptionThrottleDuration(Duration.ofMillis(100))
        .build();

    AtomicReference<Disposable> subASubscription = new AtomicReference<>();
    AtomicReference<Disposable> subBSubscription = new AtomicReference<>();

    Publisher<Set<TopicSubscription>> subscriptions = Flux.from(topicRouter.getSubscriptions())
        .log("subscriptions");
    
    StepVerifier.create(subscriptions)
        .then(() -> {        
          // Subscribe A
          subASubscription.set(topicRouter.route(source, "/sub-a").subscribe());
        })
        .thenAwait(Duration.ofMillis(300)) // Wait beyond throttle
        .expectNextMatches(subs -> subs.equals(Collections.singleton(
          new TopicSubscription(1, "/sub-a"))))
        .then(() -> {
          // Now subscribe B
          subBSubscription.set(topicRouter.route(source, "/sub-b").subscribe());
        })            
        .expectNextMatches(subs -> subs.equals(Collections.singleton(
            new TopicSubscription(2, "/sub-b"))))
        .then(() -> {
          topicRouter.complete();
          subASubscription.get().dispose();
          subBSubscription.get().dispose();
        })
        .expectComplete()
        .verify(Duration.ofSeconds(30));

  }

  @Test
  public void testSubscribeWhenConnected() {
    Flux<Integer> source = Flux.just(1, 2, 3);
    EmitterProcessor<Integer> connectedSignal = EmitterProcessor.create();
    FluxSink<Integer> connectedSink = connectedSignal.sink();
    EmitterProcessor<Integer> disconnectedSignal = EmitterProcessor.create();
    FluxSink<Integer> disconnectedSink = connectedSignal.sink();

    TopicRouter<Integer> topicRouter = TopicRouter.<Integer>builder()
        .startConnected(false)
        .connectedSignal(connectedSignal) 
        .disconnectedSignal(disconnectedSignal)
        .subscriptionThrottleDuration(Duration.ofSeconds(1))
        .build();

    Publisher<Set<TopicSubscription>> subscriptions = topicRouter.getSubscriptions();
    AtomicReference<Disposable> subASubscription = new AtomicReference<>();
    AtomicReference<Disposable> subBSubscription = new AtomicReference<>();
    AtomicReference<Disposable> subCSubscription = new AtomicReference<>();
    
    StepVerifier.create(subscriptions)
        .then(() -> {        
          // No subscription - not connected  
          subASubscription.set(topicRouter.route(source, "/sub-a").subscribe());
        })
        .expectNoEvent(Duration.ofMillis(100))
        .then(() -> {
          // No subscription - not connected
          subBSubscription.set(topicRouter.route(source, "/sub-b").subscribe());
        })
        .expectNoEvent(Duration.ofMillis(100))
        .then(() -> connectedSink.next(1))             
        .expectNextMatches(subs -> subs.equals(
            // Now connected, subscribe everything (A and B)
            new HashSet<>(Arrays.asList(
              new TopicSubscription(3, "/sub-a"), 
              new TopicSubscription(4, "/sub-b")))))
        .then(() -> {
          // Subscribe C
          subCSubscription.set(topicRouter.route(source, "/sub-c").subscribe());
        })
        .expectNextMatches(subs -> subs.equals(Collections.singleton(
            new TopicSubscription(5, "/sub-c"))))
        .then(() -> {
          // Cancel subscriber A
          log.info("Cancelling A");
          subASubscription.get().dispose();
        })
        .then(() -> disconnectedSink.next(1)) // Disconnect
        .thenAwait(Duration.ofMillis(100))
        .then(() -> connectedSink.next(1)) // Reconnect
        .expectNextMatches(subs -> subs.equals(
            // Now connected, subscribe everything (B and C)
            new HashSet<>(Arrays.asList(
              new TopicSubscription(6, "/sub-b"), 
              new TopicSubscription(7, "/sub-c")))))     
        .then(() -> {
          topicRouter.complete();
          connectedSink.complete();
          disconnectedSink.complete();
          subBSubscription.get().dispose();
          subCSubscription.get().dispose();
        })
        .expectComplete()
        .verify(Duration.ofSeconds(30));

  }

  @Test
  public void testThrottleMultipleSubscriptions() {
    Flux<Integer> source = Flux.just(1, 2, 3);

    TopicRouter<Integer> topicRouter = TopicRouter.<Integer>builder()
        .subscriptionThrottleDuration(Duration.ofSeconds(1))
        .build();

    AtomicReference<Disposable> subASubscription = new AtomicReference<>();
    AtomicReference<Disposable> subBSubscription = new AtomicReference<>();

    Publisher<Set<TopicSubscription>> subscriptions = topicRouter.getSubscriptions();
    
    StepVerifier.create(subscriptions)
        .then(() -> {        
          // Subscribe A while connected  
          subASubscription.set(topicRouter.route(source, "/sub-a").subscribe());
        })
        .then(() -> {
          // Immediately subscribe B while connected
          subBSubscription.set(topicRouter.route(source, "/sub-b").subscribe());
        })            
        .expectNextMatches(subs -> subs.equals(
            // Should batch subscriptions together
            new HashSet<>(Arrays.asList(
              new TopicSubscription(1, "/sub-a"), 
              new TopicSubscription(2, "/sub-b")))))
        .then(() -> {
          topicRouter.complete();
          subASubscription.get().dispose();
          subBSubscription.get().dispose();
        })
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }
}
