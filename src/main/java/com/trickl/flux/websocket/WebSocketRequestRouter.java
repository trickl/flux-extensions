package com.trickl.flux.websocket;

import com.trickl.exceptions.SubscriptionFailedException;
import com.trickl.flux.consumers.SimpMessageSender;

import java.text.MessageFormat;
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

  private final Map<StreamId, StreamDetail> streams = new ConcurrentHashMap<>();

  private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();

  private final Map<String, SubscriptionDetail> subscriptionDetails = new ConcurrentHashMap<>();

  private final StreamIdParser streamIdParser = new StreamIdParser();

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

  protected void subscribe(SubscriptionDetail subscription) 
      throws SubscriptionFailedException {
    String destination = subscription.getDestination();
    StreamId streamId = streamIdParser.apply(destination)
        .orElseThrow(() -> new SubscriptionFailedException(
            MessageFormat.format("Destination: {0} not found.", destination)));
    StreamDetail stream = StreamDetail.builder()
        .id(streamId)
        .build();
    
    SimpMessageSender<T> messageSender = new SimpMessageSender<>(messagingTemplate, destination);
    Flux<?> connectableFlux = this.fluxes.computeIfAbsent(
        streamId, fluxFactory::apply)    
        .orElseThrow(() -> new SubscriptionFailedException(
            MessageFormat.format("Destination: {0} not found.", destination)))
        .doOnNext(value -> handleStreamValue(stream, messageSender, value))
        .doOnSubscribe(sub -> handleStreamSubscription(stream, sub))
        .doOnCancel(() -> handleStreamCancel(stream))
        .doOnError(error -> handleStreamError(stream, error))
        .doOnComplete(() -> handleStreamComplete(stream))        
        .publish()
        .refCount();

    connectableFlux
        .doOnSubscribe(sub -> handleSubscription(subscription, stream, sub))  
        .doOnCancel(() -> handleSubscriberCancel(subscription, stream))
        .doOnError(error -> handleSubscriberError(subscription, stream, error))
        .doOnComplete(() -> handleSubscriberComplete(subscription, stream))
        .subscribe();      
  }

  protected void handleStreamSubscription(StreamDetail stream, Subscription subscription) {
    stream.setSubscriptionTime(Instant.now());
    streams.put(stream.getId(), stream);
  }

  protected void handleStreamValue(
      StreamDetail stream, SimpMessageSender<T> messageSender, T value) {
    messageSender.accept(value);
    stream.setMessageCount(stream.getMessageCount() + 1);
    stream.setLastMessageTime(Instant.now());
  }

  protected void handleStreamCancel(StreamDetail stream) {    
    stream.setCancelTime(Instant.now());
    setStreamTerminated(stream.getId());
  }

  protected void handleStreamError(StreamDetail stream, Throwable error) {
    stream.setErrorTime(Instant.now());
    stream.setErrorMessage(error.getLocalizedMessage());
    setStreamTerminated(stream.getId());
  }

  protected void handleStreamComplete(StreamDetail stream) {
    stream.setCompleteTime(Instant.now());
    setStreamTerminated(stream.getId());
  }

  protected void setStreamTerminated(StreamId id) {
    fluxes.computeIfPresent(id, (streamId, flux) -> {
      streams.computeIfPresent(streamId,
          (i, detail) -> {
            detail.setTerminated(true);
            return detail; 
          });
      return null;
    });
  }

  protected void handleSubscription(
      SubscriptionDetail detail, StreamDetail stream, Subscription subscription) {
    detail.setSubscriptionTime(Instant.now());
    stream.setSubscriberCount(stream.getSubscriberCount() + 1);
    subscriptions.put(detail.getSubscriptionId(), subscription);
    subscriptionDetails.put(detail.getSubscriptionId(), detail);
  }

  protected void handleSubscriberCancel(SubscriptionDetail subscription, StreamDetail stream) {
    stream.setSubscriberCount(stream.getSubscriberCount() - 1);
    subscription.setCancelTime(Instant.now());
  }

  protected void handleSubscriberError(
      SubscriptionDetail subscription, StreamDetail stream, Throwable error) {
    subscription.setErrorTime(Instant.now());
    subscription.setErrorMessage(error.getLocalizedMessage());
  }

  protected void handleSubscriberComplete(SubscriptionDetail subscription, StreamDetail stream) {
    subscription.setCompleteTime(Instant.now());
  }

  protected void unsubscribe(String subscriptionId) {
    subscriptions.computeIfPresent(subscriptionId,
        (id, subscription) -> {
          subscription.cancel();
          subscriptionDetails.computeIfPresent(subscriptionId,
              (subId, detail) -> {
                detail.setCancelled(true);
                return detail; 
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
        SubscriptionDetail subscriptionInfo = SubscriptionDetail.builder()
            .userName(accessor.getUser() != null ? accessor.getUser().getName() : null)
            .destination(destination)
            .subscriptionId(accessor.getSubscriptionId())
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

  public List<SubscriptionDetail> getSubscriptionDetails() {
    return subscriptionDetails.values().stream().collect(Collectors.toList());
  }

  public List<StreamDetail> getStreams() {
    return streams.values().stream().collect(Collectors.toList());
  }
}
