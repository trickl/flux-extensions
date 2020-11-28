package com.trickl.flux.routing;

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
public class TopicRouter<T> {
  private final Function<String, Predicate<T>> topicFilter;

  private final ConcatProcessor<Set<TopicSubscription>> subscriptionProcessor;

  private final ConcatProcessor<Set<TopicSubscription>> cancelProcessor;

  private final Flux<Boolean> isConnectedFlux;

  private final Disposable isConnectedSubscription;

  @Getter
  private final Publisher<Set<TopicSubscription>> subscriptions;

  @Getter
  private final Publisher<Set<TopicSubscription>> cancellations;

  private final AtomicInteger maxSubscriptionId = new AtomicInteger(0);

  private final MapRouter<T> mapRouter;

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
      Function<String, Predicate<T>> topicFilter,
      Publisher<?> connectedSignal,
      Publisher<?> disconnectedSignal,
      Duration subscriptionThrottleDuration,
      Duration cancelThrottleDuration,
      Boolean startConnected
  ) {    
    Duration subThrottleDuration = Optional.ofNullable(subscriptionThrottleDuration)
        .orElse(DEFAULT_SUBSCRIPTION_THROTTLE);
    subscriptionProcessor = ConcatProcessor.create();
    subscriptions = Flux.from(subscriptionProcessor)
        .buffer(subThrottleDuration)
        .map(list -> {
          Set<TopicSubscription> combined = list.stream()
              .flatMap(set -> set.stream())
              .collect(Collectors.toSet());
          return combined;
        });


    Duration cancelThrottlePeriod = Optional.ofNullable(cancelThrottleDuration)
         .orElse(DEFAULT_CANCEL_THROTTLE);
    cancelProcessor =  ConcatProcessor.create();

    cancellations = Flux.from(cancelProcessor)
        .buffer(cancelThrottlePeriod)
        .map(list -> {
          Set<TopicSubscription> combined = list.stream()
              .flatMap(set -> set.stream())
              .collect(Collectors.toSet());
          return combined;
        });        


    this.topicFilter = Optional.ofNullable(topicFilter).orElse(topic -> value -> true);

    Boolean connected = Optional.ofNullable(startConnected).orElse(true);
                
    Publisher<Boolean> connectedFlux = connectedSignal == null ? Mono.empty()
        : Flux.from(connectedSignal).map(value -> true)
        .log("ConnectedSignal", Level.FINE);    


    Publisher<Boolean> disconnectedFlux = disconnectedSignal == null ? Mono.empty() 
        : Flux.from(disconnectedSignal).map(value -> false)
        .log("DisconnectedSignal", Level.FINE);  

    isConnectedFlux = Flux.merge(
        Mono.just(connected),
        Flux.from(connectedFlux),
        Flux.from(disconnectedFlux))
        .log("isConnectedFlux", Level.FINE)
        .share().cache(1);

    isConnectedSubscription = isConnectedFlux.subscribe();

    mapRouter = MapRouter.<T>builder()
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
  }

  public Flux<T> route(Publisher<T> source, String topic) {
    return mapRouter.route(source, topic);
  }

  /**
   * Create a topic specific flux.
   * 
   * @param source the source flux
   * @param topic The topic name.
   * @return A flux filtered for this particular topic
  */
  protected Flux<T> create(Publisher<T> source, String topic) {
    return isConnectedFlux.flatMap(isConnected -> {
      return Flux.from(SubscriptionContextPublisher.<T, TopicSubscription>builder()
          .source(source)
          .doOnSubscribe(() -> {
            Integer id = maxSubscriptionId.incrementAndGet();
            String message = MessageFormat.format(
                "{0} subscribing to topic {1}, is connected? {2} ",
                 id, topic, isConnected);            
            log.info(message);
            TopicSubscription topicSubscription = new TopicSubscription(id, topic);
            if (isConnected) {
              subscriptionProcessor.sink().next(Collections.singleton(topicSubscription));
            }
            return topicSubscription;
          })
          .doOnCancel(topicSubscription -> {
            String message = MessageFormat.format(
                "{0} unsubscribing to topic {1}, is connected? {2} ",
                 topicSubscription.getId(), topicSubscription.getTopic(), isConnected);            
            log.info(message);
            
            if (isConnected) {
              cancelProcessor.sink().next(Collections.singleton(topicSubscription));
            }
          })
          .build())
          .filter(value -> isConnected)
          .filter(topicFilter.apply(topic));
    });
  }
}