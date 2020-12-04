package com.trickl.flux.routing;

import com.trickl.flux.monitors.SetAction;
import com.trickl.flux.monitors.SetActionType;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.java.Log;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.test.StepVerifier;

@Log
@ActiveProfiles({"unittest"})
public class TopicRouterTest {

  @Test
  public void testTopicFilters() {

    Flux<Integer> source = Flux.just(1, 2, 3, 4, 5);

    TopicRouter<Integer, String> topicRouter = TopicRouter.<Integer, String>builder()
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
    EmitterProcessor<Integer> source = EmitterProcessor.create();
    FluxSink<Integer> sourceSink = source.sink();

    TopicRouter<Integer, String> topicRouter = TopicRouter.<Integer, String>builder()
        .subscriptionThrottleDuration(Duration.ofMillis(100))
        .build();

    AtomicReference<Disposable> subASubscription = new AtomicReference<>();
    AtomicReference<Disposable> subBSubscription = new AtomicReference<>();

    Publisher<SetAction<TopicSubscription<String>>> subscriptions
        = Flux.from(topicRouter.getSubscriptionActions())
        .log("subscriptions");
    
    StepVerifier.create(subscriptions)
        .then(() -> {        
          // Subscribe A
          subASubscription.set(topicRouter.route(source, "/sub-a").subscribe());
        })
        .thenAwait(Duration.ofMillis(300)) // Wait beyond throttle
        .expectNextMatches(subs -> subs.equals(new SetAction<>(SetActionType.Add,
            Collections.singleton(
            new TopicSubscription<String>(1, "/sub-a")),
            Collections.singleton(
            new TopicSubscription<String>(1, "/sub-a"))
          )))
        .then(() -> {
          // Now subscribe B
          subBSubscription.set(topicRouter.route(source, "/sub-b").subscribe());
        })            
        .expectNextMatches(subs -> subs.equals(new SetAction<>(SetActionType.Add,
            Collections.singleton(
            new TopicSubscription<String>(2, "/sub-b")),
            new HashSet<>(Arrays.asList(
              new TopicSubscription<String>(1, "/sub-a"), 
              new TopicSubscription<String>(2, "/sub-b"))
              ))))
        .then(() -> {
          subASubscription.get().dispose();
          subBSubscription.get().dispose();
        })
        .expectNextMatches(subs -> subs.equals(new SetAction<>(SetActionType.Remove,            
              // Now connected, subscribe everything (B and C)
              new HashSet<>(Arrays.asList(
                  new TopicSubscription<String>(1, "/sub-a"), 
                  new TopicSubscription<String>(2, "/sub-b"))),
              Collections.emptySet())))
        .then(() -> {
          topicRouter.complete();
          sourceSink.complete();
        })
        .expectComplete()
        .verify(Duration.ofSeconds(30));

  }

  @Test
  public void testSubscribeWhenConnected() {
    EmitterProcessor<Integer> source = EmitterProcessor.create();
    FluxSink<Integer> sourceSink = source.sink();
    EmitterProcessor<Integer> connectedSignal = EmitterProcessor.create();
    FluxSink<Integer> connectedSink = connectedSignal.sink();
    EmitterProcessor<Integer> disconnectedSignal = EmitterProcessor.create();
    FluxSink<Integer> disconnectedSink = disconnectedSignal.sink();

    TopicRouter<Integer, String> topicRouter = TopicRouter.<Integer, String>builder()
        .startConnected(false)
        .connectedSignal(connectedSignal) 
        .disconnectedSignal(disconnectedSignal)
        .subscriptionThrottleDuration(Duration.ofSeconds(1))
        .build();

    Publisher<SetAction<TopicSubscription<String>>> subscriptions
         = topicRouter.getSubscriptionActions();
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
        .expectNextMatches(subs -> subs.equals(new SetAction<>(SetActionType.Add,            
            new HashSet<>(Arrays.asList(
                new TopicSubscription<String>(1, "/sub-a"), 
                new TopicSubscription<String>(2, "/sub-b"))),
            new HashSet<>(Arrays.asList(
              new TopicSubscription<String>(1, "/sub-a"), 
              new TopicSubscription<String>(2, "/sub-b")))
        )))
        .expectNoEvent(Duration.ofMillis(100))
        .then(() -> {
          // Subscribe C
          subCSubscription.set(topicRouter.route(source, "/sub-c").subscribe());
        })
        .expectNextMatches(subs -> subs.equals(new SetAction<>(SetActionType.Add,            
            Collections.singleton(
          new TopicSubscription<String>(3, "/sub-c")),
            new HashSet<>(Arrays.asList(
              new TopicSubscription<String>(1, "/sub-a"), 
              new TopicSubscription<String>(2, "/sub-b"),
              new TopicSubscription<String>(3, "/sub-c")))
        )))
        .expectNoEvent(Duration.ofMillis(100))
        .then(() -> {
          // Cancel subscriber A
          subASubscription.get().dispose();
        })
        .expectNextMatches(subs -> subs.equals(new SetAction<>(SetActionType.Remove,
            Collections.singleton(
            new TopicSubscription<String>(1, "/sub-a")),
            new HashSet<>(Arrays.asList(
              new TopicSubscription<String>(2, "/sub-b"), 
              new TopicSubscription<String>(3, "/sub-c"))
              ))))
        .expectNoEvent(Duration.ofMillis(100))
        .then(() -> disconnectedSink.next(1)) // Disconnect
        .expectNextMatches(subs -> subs.equals(new SetAction<>(SetActionType.Clear,            
              Collections.emptySet(),
              Collections.emptySet()
        )))
        .thenAwait(Duration.ofMillis(100))
        .then(() -> connectedSink.next(1)) // Reconnect  
        .expectNextMatches(subs -> subs.equals(new SetAction<>(SetActionType.Add,            
              // Now connected, subscribe everything (B and C)
              new HashSet<>(Arrays.asList(
                  new TopicSubscription<String>(4, "/sub-b"), 
                  new TopicSubscription<String>(5, "/sub-c"))),
              new HashSet<>(Arrays.asList(
                new TopicSubscription<String>(4, "/sub-b"), 
                new TopicSubscription<String>(5, "/sub-c")))
        )))
        .then(() -> {          
          log.info("Completing");          
        })
        .then(() -> {
          subBSubscription.get().dispose();
          subCSubscription.get().dispose();
        })
        .expectNextMatches(subs -> subs.equals(new SetAction<>(SetActionType.Remove,        
          new HashSet<>(Arrays.asList(
                new TopicSubscription<String>(4, "/sub-b"), 
                new TopicSubscription<String>(5, "/sub-c"))),
          Collections.emptySet())))
        .then(() -> {
          topicRouter.complete();
          sourceSink.complete();
          connectedSink.complete();
          disconnectedSink.complete();
        })
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }

  @Test
  public void testThrottleMultipleSubscriptions() {
    EmitterProcessor<Integer> source = EmitterProcessor.create();
    FluxSink<Integer> sourceSink = source.sink();

    TopicRouter<Integer, String> topicRouter = TopicRouter.<Integer, String>builder()
        .subscriptionThrottleDuration(Duration.ofSeconds(1))
        .build();

    AtomicReference<Disposable> subASubscription = new AtomicReference<>();
    AtomicReference<Disposable> subBSubscription = new AtomicReference<>();

    Publisher<SetAction<TopicSubscription<String>>> subscriptions 
        = topicRouter.getSubscriptionActions();
    
    StepVerifier.create(subscriptions)
        .then(() -> {        
          // Subscribe A while connected  
          subASubscription.set(topicRouter.route(source, "/sub-a").subscribe());
        })
        .then(() -> {
          // Immediately subscribe B while connected
          subBSubscription.set(topicRouter.route(source, "/sub-b").subscribe());
        })
        .expectNextMatches(subs -> subs.equals(new SetAction<>(SetActionType.Add,            
            new HashSet<>(Arrays.asList(
                new TopicSubscription<String>(1, "/sub-a"), 
                new TopicSubscription<String>(2, "/sub-b"))),
            new HashSet<>(Arrays.asList(
              new TopicSubscription<String>(1, "/sub-a"), 
              new TopicSubscription<String>(2, "/sub-b")))
        )))              
        .then(() -> {
          subASubscription.get().dispose();
          subBSubscription.get().dispose();
        })
        .expectNextMatches(subs -> subs.equals(new SetAction<>(SetActionType.Remove,            
              // Now connected, subscribe everything (B and C)
              new HashSet<>(Arrays.asList(
                  new TopicSubscription<String>(1, "/sub-a"), 
                  new TopicSubscription<String>(2, "/sub-b"))),
              Collections.emptySet())))                      
        .then(() -> {
          topicRouter.complete();
          sourceSink.complete();
        })
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }
}
