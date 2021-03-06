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
    log.info("Handling subscription " + subscription.getDestination());

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
            .computeIfAbsent(streamId, id -> {
              return fluxFactory.apply(id).map(flux -> flux
                  .doOnNext(value -> handleStreamValue(subscription, messageSender, value))
                  .doOnSubscribe(sub -> handleStreamSubscription(subscription))
                  .doOnCancel(() -> handleStreamCancel(id, subscription))
                  .doOnError(error -> handleStreamError(id, subscription, error))
                  .doOnComplete(() -> handleStreamComplete(id, subscription))
                  .publishOn(Schedulers.parallel())
                  .publish()
                  .refCount(1, streamDisconnectGracePeriod));
            })
            .orElseThrow(
              () ->
                  new SubscriptionFailedException(
                      MessageFormat.format("Destination: {0} not found.", destination)));

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
    log.info("Stream cancelled " + streamId.toString());
    subscription.setCancelTime(Instant.now());
    setStreamTerminated(streamId, subscription.getDestination());
  }

  protected void handleStreamError(StreamId streamId, 
      SubscriptionDetails subscription, Throwable error) {
    log.log(Level.WARNING, "Stream error " + streamId.toString(), error);
    subscription.setErrorTime(Instant.now());
    subscription.setErrorMessage(error.toString());
    setStreamTerminated(streamId, subscription.getDestination());
  }

  protected void handleStreamComplete(StreamId streamId, SubscriptionDetails subscription) {
    log.info("Stream completed " + streamId.toString());
    subscription.setCompleteTime(Instant.now());
    setStreamTerminated(streamId, subscription.getDestination());
  }

  protected void setStreamTerminated(StreamId streamId, String destination) {
    log.info("Marking stream as terminated " + streamId.toString());
    fluxes.computeIfPresent(
        streamId,
        (id, flux) -> {
          subscriptionDetails.computeIfPresent(
              destination,
              (i, detail) -> {
                detail.setTerminated(true);
                return detail;
              });
          return null;
        });
  }

  protected void handleConnection(SessionDetails session) {
    log.info("Handling connection event with sessionId - " + session.getId());
    session.setConnectionTime(Instant.now());
    sessionDetails.put(session.getId(), session);
  }

  protected void handleStreamSubscription(SubscriptionDetails subscription) {
    log.info("Handling stream subscription " + subscription.getDestination());
    subscription.setSubscriptionTime(Instant.now());
    subscriptionDetails.put(subscription.getDestination(), subscription);
  }

  protected void handleFluxSubscription(SubscriptionDetails subscription,
      Subscription fluxSubscription) {
    log.info("Handling flux subscription " + subscription.getDestination());
    subscription.setSubscriberCount(subscription.getSubscriberCount() + 1);
    subscriptions.put(subscription.getDestination(), fluxSubscription);
  }

  protected void handleFluxSubscriberCancel(SubscriptionDetails subscription) {
    log.log(Level.INFO, "Subscriber cancelled " + subscription.toString());
    subscription.setSubscriberCount(subscription.getSubscriberCount() - 1);
    subscription.setCancelTime(Instant.now());
  }

  protected void handleFluxSubscriberError(SubscriptionDetails subscription, Throwable error) {    
    log.log(Level.WARNING, "Subscriber error " + subscription.toString(), error);
    subscription.setErrorTime(Instant.now());
    subscription.setErrorMessage(error.toString());
  }

  protected void handleFluxSubscriberComplete(SubscriptionDetails subscription) {
    log.log(Level.INFO, "Subscriber complete " + subscription.toString());
    subscription.setCompleteTime(Instant.now());
  }

  /** Cancel all subscriptions. */
  public void unsubscribeAll() {
    log.log(Level.INFO, "Unsubscribing all.");
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
   * @param destination The subscription to unsubscribe
   */
  public void unsubscribe(String destination) {
    log.info("Unsubscribing " + destination);
    subscriptionDetails.computeIfPresent(
        destination,
        (id, subscription) -> {
          subscription.setUnsubscribeTime(Instant.now());
          return subscription;
        });

    subscriptions.computeIfPresent(
        destination,
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
        if (!sessionDetails.containsKey(sessionId)) {
          log.warning("Received a subscribe without a corresponding session - " + sessionId);
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
          .map(SubscriptionDetails::getDestination)
          .forEach(this::unsubscribe);

    } else if (event instanceof SessionUnsubscribeEvent) {
      unsubscribe(accessor.getSubscriptionId());
    }
  }

  /**
   * Get all session details.
   * 
   * @param time Ignore older sessions before this time.
   * @return A list of session details.
   */
  public List<SessionDetails> getSessionDetailsByUpdateTimeAfter(Instant time) {
    return sessionDetails.values()
        .stream()
        .filter((SessionDetails details) -> details.getDisconnectionTime() == null
          || details.getDisconnectionTime().isAfter(time)           
        )   
        .sorted((SessionDetails a, SessionDetails b) -> {
          Instant disconnectionTimeA = a.getDisconnectionTime();
          Instant disconnectionTimeB = b.getDisconnectionTime();
          if (disconnectionTimeA != null && disconnectionTimeB != null) {
            return disconnectionTimeB.compareTo(disconnectionTimeA);
          } else if (disconnectionTimeA != null) {
            return 1;
          } else if (disconnectionTimeB != null) {
            return -1;
          } else {
            return b.getConnectionTime().compareTo(a.getConnectionTime());
          }
        })
        .collect(Collectors.toList());
  }

  /**
   * Get all subscriptions.
   * 
   * @param time Ignore older subscriptions before this time.
   * @return A list of session details
   */
  public List<SubscriptionDetails> getSubscriptionsByUpdateTimeAfter(Instant time) {
    return subscriptionDetails.values()
        .stream()
        .filter((SubscriptionDetails details) -> getTerminationTime(details) == null
          || getTerminationTime(details).isAfter(time)        
        )   
        .sorted((SubscriptionDetails a, SubscriptionDetails b) -> {
          Instant terminationTimeA = getTerminationTime(a);
          Instant terminationTimeB = getTerminationTime(b);
          if (terminationTimeA != null && terminationTimeB != null) {
            return terminationTimeB.compareTo(terminationTimeA);
          } else if (terminationTimeA != null) {
            return 1;
          } else if (terminationTimeB != null) {
            return -1;
          } else {
            return b.getSubscriptionTime().compareTo(a.getSubscriptionTime());
          }
        }).collect(Collectors.toList());
  }

  /**
   * Get subscription termination time.
   * 
   * @param subscriptionDetails The subscription.
   * @return The time the subscription terminated.
   */
  public static Instant getTerminationTime(SubscriptionDetails subscriptionDetails) {
    if (subscriptionDetails.getErrorTime() != null) {
      return subscriptionDetails.getErrorTime();
    } else if (subscriptionDetails.getCancelTime() != null) {
      return subscriptionDetails.getCancelTime();
    } else if (subscriptionDetails.getCompleteTime() != null) {
      return subscriptionDetails.getCompleteTime();
    }
    return null;
  }
}
