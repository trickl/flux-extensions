package com.trickl.flux.websocket;

import com.trickl.exceptions.NoSuchStreamException;
import com.trickl.exceptions.SubscriptionFailedException;
import com.trickl.flux.consumers.SimpMessageSender;
import com.trickl.model.streams.StreamDetails;
import com.trickl.model.streams.StreamId;
import com.trickl.model.streams.SubscriptionDetails;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

import org.reactivestreams.Subscription;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.event.SmartApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.messaging.simp.user.SimpSubscription;
import org.springframework.messaging.simp.user.SimpUserRegistry;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.util.Assert;
import org.springframework.web.socket.messaging.AbstractSubProtocolEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;
import org.springframework.web.socket.messaging.SessionUnsubscribeEvent;

import reactor.core.publisher.Flux;

@Log
@RequiredArgsConstructor
public class WebSocketRequestRouter<T> implements SmartApplicationListener {

  private final Function<StreamId, Optional<Flux<T>>> fluxFactory;

  private final SimpUserRegistry simpUserRegistry;

  private final SimpMessagingTemplate messagingTemplate;

  private final Map<StreamId, Optional<Flux<T>>> fluxes = new ConcurrentHashMap<>();

  private final Map<String, StreamDetails> streams = new ConcurrentHashMap<>();

  private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();

  private final Map<String, SubscriptionDetails> subscriptionDetails = new ConcurrentHashMap<>();

  private final StreamIdParser streamIdParser = new StreamIdParser();

  private final Duration streamDisconnectGracePeriod = Duration.ofSeconds(5);

  @Override
  public boolean supportsEventType(Class<? extends ApplicationEvent> eventType) {
    return AbstractSubProtocolEvent.class.isAssignableFrom(eventType);
  }

  @Override
  public boolean supportsSourceType(@Nullable Class<?> sourceType) {
    return true;
  }

  @Override
  public int getOrder() {
    return Ordered.LOWEST_PRECEDENCE;
  }

  protected void subscribe(SubscriptionDetails subscription) 
      throws SubscriptionFailedException {
    String destination = subscription.getDestination();
    StreamId streamId = streamIdParser.apply(destination)
        .orElseThrow(() -> new SubscriptionFailedException(
            MessageFormat.format("Destination: {0} not found.", destination)));
    StreamDetails stream = StreamDetails.builder()
        .destination(destination)
        .build();
    
    SimpMessageSender<T> messageSender = new SimpMessageSender<>(messagingTemplate, destination);
    Flux<?> connectableFlux = this.fluxes.computeIfAbsent(
        streamId, fluxFactory::apply)    
        .orElseThrow(() -> new SubscriptionFailedException(
            MessageFormat.format("Destination: {0} not found.", destination)))
        .doOnNext(value -> handleStreamValue(stream, messageSender, value))
        .doOnSubscribe(sub -> handleStreamSubscription(stream, sub))
        .doOnCancel(() -> handleStreamCancel(streamId, stream))
        .doOnError(error -> handleStreamError(streamId, stream, error))
        .doOnComplete(() -> handleStreamComplete(streamId, stream))        
        .publish()
        .refCount(1, streamDisconnectGracePeriod);

    connectableFlux
        .doOnSubscribe(sub -> handleSubscription(subscription, stream, sub))  
        .doOnCancel(() -> handleSubscriberCancel(subscription, stream))
        .doOnError(error -> handleSubscriberError(subscription, stream, error))
        .doOnComplete(() -> handleSubscriberComplete(subscription, stream))
        .subscribe();      
  }

  protected void handleStreamSubscription(StreamDetails stream, Subscription subscription) {
    stream.setSubscriptionTime(Instant.now());
    streams.put(stream.getDestination(), stream);
  }

  protected void handleStreamValue(
      StreamDetails stream, SimpMessageSender<T> messageSender, T value) {
    messageSender.accept(value);
    stream.setMessageCount(stream.getMessageCount() + 1);
    stream.setLastMessageTime(Instant.now());
  }

  protected void handleStreamCancel(StreamId streamId,StreamDetails stream) {    
    stream.setCancelTime(Instant.now());
    setStreamTerminated(streamId, stream.getDestination());
  }

  protected void handleStreamError(StreamId streamId,StreamDetails stream, Throwable error) {
    stream.setErrorTime(Instant.now());
    stream.setErrorMessage(error.getLocalizedMessage());
    setStreamTerminated(streamId, stream.getDestination());
  }

  protected void handleStreamComplete(StreamId streamId, StreamDetails stream) {
    stream.setCompleteTime(Instant.now());
    setStreamTerminated(streamId, stream.getDestination());
  }

  protected void setStreamTerminated(StreamId streamId, String destination) {
    fluxes.computeIfPresent(streamId, (id, flux) -> {
      streams.computeIfPresent(destination,
          (i, detail) -> {
            detail.setTerminated(true);
            return detail; 
          });
      return null;
    });
  }

  protected void handleSubscription(
      SubscriptionDetails detail, StreamDetails stream, Subscription subscription) {
    detail.setSubscriptionTime(Instant.now());
    stream.setSubscriberCount(stream.getSubscriberCount() + 1);
    subscriptions.put(detail.getId(), subscription);
    subscriptionDetails.put(detail.getId(), detail);
  }

  protected void handleSubscriberCancel(SubscriptionDetails subscription, StreamDetails stream) {
    stream.setSubscriberCount(stream.getSubscriberCount() - 1);
    subscription.setCancelTime(Instant.now());
  }

  protected void handleSubscriberError(
      SubscriptionDetails subscription, StreamDetails stream, Throwable error) {
    subscription.setErrorTime(Instant.now());
    subscription.setErrorMessage(error.getLocalizedMessage());
  }

  protected void handleSubscriberComplete(SubscriptionDetails subscription, StreamDetails stream) {
    subscription.setCompleteTime(Instant.now());
  }

  /**
   * Cancel all subscriptions.
   */
  public void unsubscribeAll() {
    subscriptions.keySet().forEach(this::unsubscribe);
  }

  /**
   * Cancel a subscription.
   * @param subscriptionId The subscription to unsubscribe
   */
  public void unsubscribe(String subscriptionId) {
    subscriptions.computeIfPresent(subscriptionId,
        (id, subscription) -> {
          subscription.cancel();
          subscriptionDetails.computeIfPresent(subscriptionId,
              (subId, details) -> {
                details.setCancelTime(Instant.now());
                return details; 
              });
          return null;
        });
  }

  @Override
  public void onApplicationEvent(ApplicationEvent event) {
    log.log(Level.FINE, "Received WebSocket event - " + event.getClass());
    AbstractSubProtocolEvent subProtocolEvent = (AbstractSubProtocolEvent) event;
    Message<?> message = subProtocolEvent.getMessage();

    SimpMessageHeaderAccessor accessor =
        MessageHeaderAccessor.getAccessor(message, SimpMessageHeaderAccessor.class);
    Assert.state(accessor != null, "No SimpMessageHeaderAccessor");
    
    if (event instanceof SessionSubscribeEvent) {
      String destination = accessor.getDestination();
      Assert.state(destination != null, "No destination");
      
      try {
        SubscriptionDetails subscriptionInfo = SubscriptionDetails.builder()
            .userName(accessor.getUser() != null ? accessor.getUser().getName() : null)
            .id(accessor.getSubscriptionId())
            .destination(destination)            
            .sessionId(accessor.getSessionId())
            .build();
        subscribe(subscriptionInfo);
      } catch (SubscriptionFailedException ex) {
        log.log(Level.WARNING, "Subscription failed", ex);
      }
    } else if (event instanceof SessionDisconnectEvent) {
      // Unsubscribe any hanging subscriptions
      String sessionId = accessor.getSessionId();
      simpUserRegistry
          .findSubscriptions(
              subscription -> subscription.getSession().getId().equals(sessionId))
          .stream()
          .map(SimpSubscription::getId)
          .forEach(this::unsubscribe);

    } else if (event instanceof SessionUnsubscribeEvent) {
      unsubscribe(accessor.getSubscriptionId());
    }
  }

  public List<SubscriptionDetails> getSubscriptionDetails() {
    return subscriptionDetails.values().stream().collect(Collectors.toList());
  }

  public List<StreamDetails> getStreams() {
    return streams.values().stream().collect(Collectors.toList());
  }
}
