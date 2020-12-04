package com.trickl.flux.routing;

import com.trickl.flux.monitors.SetAction;
import com.trickl.flux.monitors.SetMonitor;
import com.trickl.flux.publishers.ConcatProcessor;
import com.trickl.flux.publishers.SubscriptionContextPublisher;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
public class TopicRouter<T, TopicT> {
  private final Function<TopicT, Predicate<T>> topicFilter;

  private final ConcatProcessor<Set<TopicSubscription<TopicT>>> subscriptionProcessor;

  private final ConcatProcessor<Set<TopicSubscription<TopicT>>> unsubscribeProcessor;

  private final Flux<Boolean> isConnectedFlux;

  private final Disposable isConnectedSubscription;

  @Getter
  private final Publisher<SetAction<TopicSubscription<TopicT>>> subscriptionActions;

  private final AtomicInteger maxSubscriptionId = new AtomicInteger(0);

  private final AtomicInteger lastDisconnectionMaxId = new AtomicInteger(0);

  private final MapRouter<T, TopicT> mapRouter;

  private static final Duration DEFAULT_SUBSCRIPTION_THROTTLE = Duration.ofSeconds(1);

  private static final Duration DEFAULT_CANCEL_THROTTLE = Duration.ofSeconds(1);

  /**
   * Build a new topic flux router.
   * 
   * @param topicFilter Only listen to certain topics.
   * @param connectedSignal Signal for connection.
   * @param disconnectedSignal Signal for disconnecion.
   * @param subscriptionThrottleDuration Debounce subscriptions.
   * @param cancelThrottleDuration Debounce cancelations.
   * @param startConnected Are we connected upon construction?
   */
  @Builder
  public TopicRouter(
      Function<TopicT, Predicate<T>> topicFilter,
      Publisher<?> connectedSignal,
      Publisher<?> disconnectedSignal,
      Duration subscriptionThrottleDuration,
      Duration cancelThrottleDuration,
      Boolean startConnected
  ) {    
    Duration subThrottleDuration = Optional.ofNullable(subscriptionThrottleDuration)
        .orElse(DEFAULT_SUBSCRIPTION_THROTTLE);
    subscriptionProcessor = ConcatProcessor.create();

    Publisher<Set<TopicSubscription<TopicT>>> subscriptions = Flux.from(subscriptionProcessor)
        .buffer(subThrottleDuration)
        .map(list -> {
          Set<TopicSubscription<TopicT>> combined = list.stream()
              .flatMap(set -> set.stream())
              .collect(Collectors.toSet());
          return combined;
        });


    Duration cancelThrottlePeriod = Optional.ofNullable(cancelThrottleDuration)
         .orElse(DEFAULT_CANCEL_THROTTLE);
    unsubscribeProcessor =  ConcatProcessor.create();

    Publisher<Set<TopicSubscription<TopicT>>> unsubscriptions = Flux.from(unsubscribeProcessor)
        .buffer(cancelThrottlePeriod)
        .map(list -> {
          Set<TopicSubscription<TopicT>> combined = list.stream()
              .flatMap(set -> set.stream())
              .collect(Collectors.toSet());
          return combined;
        });
        
        
    subscriptionActions = SetMonitor.<TopicSubscription<TopicT>>builder()
        .addPublisher(subscriptions)
        .removePublisher(unsubscriptions)
        .clearPublisher(Optional.ofNullable(disconnectedSignal).orElse(Mono.empty()))
        .build();

    this.topicFilter = Optional.ofNullable(topicFilter).orElse(topic -> value -> true);

    Boolean connected = Optional.ofNullable(startConnected).orElse(true);
                
    Publisher<Boolean> connectedFlux = connectedSignal == null ? Mono.empty()
        : Flux.from(connectedSignal).map(value -> true)
        .log("ConnectedSignal", Level.FINE);    


    Publisher<Boolean> disconnectedFlux = disconnectedSignal == null ? Mono.empty() 
        : Flux.from(disconnectedSignal).map(value -> false)
        .doOnNext(signal -> {
          lastDisconnectionMaxId.set(maxSubscriptionId.get());
        })
        .log("DisconnectedSignal", Level.FINE);  

    isConnectedFlux = Flux.merge(
        Mono.just(connected),
        Flux.from(connectedFlux),
        Flux.from(disconnectedFlux))
        .log("isConnectedFlux", Level.FINE)
        .share().cache(1);

    isConnectedSubscription = isConnectedFlux.subscribe();

    mapRouter = MapRouter.<T, TopicT>builder()
        .fluxCreator(this::create)
        .build();
  }

  /**
   * Complete all subscriptions.
   */
  public void complete() {
    if (!isConnectedSubscription.isDisposed()) {
      isConnectedSubscription.dispose();
    }
    subscriptionProcessor.complete();
    unsubscribeProcessor.complete();
  }

  public Flux<T> route(Publisher<T> source, TopicT topic) {
    return mapRouter.route(source, topic);
  }

  /**
   * Create a topic specific flux.
   * 
   * @param source the source flux
   * @param topic The topic name.
   * @return A flux filtered for this particular topic
  */
  protected Flux<T> create(Publisher<T> source, TopicT topic) {
    return isConnectedFlux.switchMap(isConnected -> {
      return Flux.from(SubscriptionContextPublisher.<T, TopicSubscription<TopicT>>builder()
          .source(source)
          .doOnSubscribe(() -> handleOnSubscribe(topic, isConnected))
          .doOnCancel(topicSubscription -> handleOnUnsubscribe(topicSubscription))
          .doOnTerminate(topicSubscription -> handleOnUnsubscribe(topicSubscription))
          .build())
          .share()
          //.filter(value -> isConnected)
          .filter(topicFilter.apply(topic));
    });
  }

  private TopicSubscription<TopicT> handleOnSubscribe(
      TopicT topic,
      boolean isConnected) {
    Integer id = isConnected ? maxSubscriptionId.incrementAndGet() : -1;
    String message = MessageFormat.format(
        "{0} subscribing to topic {1}, is connected? {2} ",
          id, topic, isConnected);            
    log.info(message);
    TopicSubscription<TopicT> topicSubscription = new TopicSubscription<TopicT>(id, topic);
    if (isConnected) {
      subscriptionProcessor.sink().next(Collections.singleton(topicSubscription));
    }
    return topicSubscription;
  }

  private void handleOnUnsubscribe(TopicSubscription<TopicT> topicSubscription) {    
    if (topicSubscription.getId() > lastDisconnectionMaxId.get()) {
      String message = MessageFormat.format(
          "{0} unsubscribing to topic {1}",
          topicSubscription.getId(), topicSubscription.getTopic());            
      log.info(message);  
      unsubscribeProcessor.sink().next(Collections.singleton(topicSubscription));
    }
  }
}