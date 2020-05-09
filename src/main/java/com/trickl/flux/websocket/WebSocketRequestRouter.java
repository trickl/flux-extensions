package com.trickl.flux.websocket;

import com.trickl.exceptions.ConnectionFailedException;
import com.trickl.exceptions.SubscriptionFailedException;
import com.trickl.flux.consumers.SimpMessageSender;
import com.trickl.model.streams.SessionDetails;
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
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.util.Assert;
import org.springframework.web.socket.messaging.AbstractSubProtocolEvent;
import org.springframework.web.socket.messaging.SessionConnectEvent;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;
import org.springframework.web.socket.messaging.SessionUnsubscribeEvent;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Log
@RequiredArgsConstructor
public class WebSocketRequestRouter<T> implements SmartApplicationListener {

  private final Function<StreamId, Optional<Flux<T>>> fluxFactory;

  private final SimpMessagingTemplate messagingTemplate;

  private final Map<StreamId, Optional<Flux<T>>> fluxes = new ConcurrentHashMap<>();

  private final Map<String, SubscriptionDetails> subscriptionDetails = new ConcurrentHashMap<>();

  private final Map<String, Subscription> subscriptions = new ConcurrentHashMap<>();

  private final Map<String, SessionDetails> sessionDetails = new ConcurrentHashMap<>();

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

  protected void handleSessionConnect(SessionDetails session) throws ConnectionFailedException {
    handleConnection(session);
  }

  protected void handleSubscription(SubscriptionDetails subscription) 
      throws SubscriptionFailedException {
    log.info("Handling subscription " + subscription.getId());

    String destination = subscription.getDestination();
    SimpMessageSender<T> messageSender = new SimpMessageSender<>(messagingTemplate,
        destination);
    StreamId streamId = streamIdParser
        .apply(destination)
        .orElseThrow(
            () ->
                new SubscriptionFailedException(
                    MessageFormat.format("Destination: {0} not found.", 
                    destination)));
    Flux<?> connectableFlux =
        this.fluxes
            .computeIfAbsent(streamId, fluxFactory::apply)
            .orElseThrow(
                () ->
                    new SubscriptionFailedException(
                        MessageFormat.format("Destination: {0} not found.", destination)))
            .doOnNext(value -> handleStreamValue(subscription, messageSender, value))
            .doOnSubscribe(sub -> handleStreamSubscription(subscription))
            .doOnCancel(() -> handleStreamCancel(streamId, subscription))
            .doOnError(error -> handleStreamError(streamId, subscription, error))
            .doOnComplete(() -> handleStreamComplete(streamId, subscription))
            .publishOn(Schedulers.parallel())
            .publish()
            .refCount(1, streamDisconnectGracePeriod);

    connectableFlux
        .doOnSubscribe(sub -> handleFluxSubscription(subscription, sub))
        .doOnCancel(() -> handleFluxSubscriberCancel(subscription))
        .doOnError(error -> handleFluxSubscriberError(subscription, error))
        .doOnComplete(() -> handleFluxSubscriberComplete(subscription))
        .subscribeOn(Schedulers.parallel())
        .subscribe();
  }
  
  protected void handleStreamValue(
      SubscriptionDetails stream, SimpMessageSender<T> messageSender, T value) {
    messageSender.accept(value);
    stream.setMessageCount(stream.getMessageCount() + 1);
    stream.setLastMessageTime(Instant.now());
  }

  protected void handleStreamCancel(StreamId streamId, SubscriptionDetails subscription) {
    subscription.setCancelTime(Instant.now());
    setStreamTerminated(streamId, subscription.getId());
  }

  protected void handleStreamError(StreamId streamId, 
      SubscriptionDetails subscription, Throwable error) {
    subscription.setErrorTime(Instant.now());
    subscription.setErrorMessage(error.toString());
    setStreamTerminated(streamId, subscription.getId());
  }

  protected void handleStreamComplete(StreamId streamId, SubscriptionDetails subscription) {
    subscription.setCompleteTime(Instant.now());
    setStreamTerminated(streamId, subscription.getId());
  }

  protected void setStreamTerminated(StreamId streamId, String subscriptionId) {
    log.info("Marking stream as terminated " + streamId.toString());
    fluxes.computeIfPresent(
        streamId,
        (id, flux) -> {
          subscriptionDetails.computeIfPresent(
              subscriptionId,
              (i, detail) -> {
                detail.setTerminated(true);
                return detail;
              });
          return null;
        });
  }

  protected void handleConnection(SessionDetails session) {
    session.setConnectionTime(Instant.now());
    sessionDetails.put(session.getId(), session);
  }

  protected void handleStreamSubscription(SubscriptionDetails stream) {
    log.info("Handling stream subscription " + stream.getId());
    stream.setSubscriptionTime(Instant.now());
    subscriptionDetails.put(stream.getDestination(), stream);
  }

  protected void handleFluxSubscription(SubscriptionDetails subscription,
      Subscription fluxSubscription) {
    log.info("Handling flux subscription " + subscription.getId());
    subscription.setSubscriberCount(subscription.getSubscriberCount() + 1);
    subscriptions.put(subscription.getId(), fluxSubscription);
  }

  protected void handleFluxSubscriberCancel(SubscriptionDetails subscription) {
    subscription.setSubscriberCount(subscription.getSubscriberCount() - 1);
    subscription.setCancelTime(Instant.now());
  }

  protected void handleFluxSubscriberError(SubscriptionDetails subscription, Throwable error) {    
    subscription.setErrorTime(Instant.now());
    subscription.setErrorMessage(error.toString());
  }

  protected void handleFluxSubscriberComplete(SubscriptionDetails subscription) {
    subscription.setCompleteTime(Instant.now());
  }

  /** Cancel all subscriptions. */
  public void unsubscribeAll() {
    subscriptionDetails.keySet().forEach(this::unsubscribe);
  }

  /**
   * Disconnect a session.
   *
   * @param sessionId The session to cancel
   */
  public void disconnect(String sessionId) {
    sessionDetails.computeIfPresent(
        sessionId,
        (id, session) -> {
        session.setDisconnectionTime(Instant.now());
        return session;
      });
  }

  /**
   * Cancel a subscription.
   *
   * @param subscriptionId The subscription to unsubscribe
   */
  public void unsubscribe(String subscriptionId) {
    log.info("Unsubscribing " + subscriptionId);
    subscriptionDetails.computeIfPresent(
        subscriptionId,
        (id, subscription) -> {
          subscription.setUnsubscribeTime(Instant.now());
          return subscription;
        });

    subscriptions.computeIfPresent(
        subscriptionId,
        (id, subscription) -> {
          subscription.cancel();          
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

    if (event instanceof SessionConnectEvent) {
      String sessionId = accessor.getSessionId();
      Assert.state(sessionId != null, "No sessionId");
      SessionDetails session =
            SessionDetails.builder()
                .userName(accessor.getUser() != null ? accessor.getUser().getName() : null)
                .id(sessionId)
                .build();

      try {
        handleSessionConnect(session);        
      } catch (ConnectionFailedException ex) {
        log.log(Level.WARNING, "Connect failed", ex);
      }
    } else if (event instanceof SessionSubscribeEvent) {      
      String sessionId = accessor.getSessionId();
      String subscriptionId = accessor.getSubscriptionId();
      String destination = accessor.getDestination();
      Assert.state(sessionId != null, "No sessionId");
      Assert.state(subscriptionId != null, "No subscriptionId");
      Assert.state(destination != null, "No destination");

      SubscriptionDetails subscription =
          SubscriptionDetails.builder()
                .id(subscriptionId)
                .sessionId(sessionId)
                .destination(destination)
                .build();
      
      try {        
        if (sessionDetails.containsKey(sessionId)) {
          log.warning("Received a subscribe without a corresponding session");
        }

        handleSubscription(subscription);        
      } catch (SubscriptionFailedException ex) {
        log.log(Level.WARNING, "Connect failed", ex);
      }
    } else if (event instanceof SessionDisconnectEvent) {
      String sessionId = accessor.getSessionId();
      disconnect(sessionId);

      subscriptionDetails.values().stream()
          .filter(sub -> sub.getSessionId() == sessionId)
          .map(SubscriptionDetails::getId)
          .forEach(this::unsubscribe);

    } else if (event instanceof SessionUnsubscribeEvent) {
      unsubscribe(accessor.getSubscriptionId());
    }
  }

  public List<SessionDetails> getSessionDetails() {
    return sessionDetails.values().stream().collect(Collectors.toList());
  }

  public List<SubscriptionDetails> getSubscriptions() {
    return subscriptionDetails.values().stream().collect(Collectors.toList());
  }
}
