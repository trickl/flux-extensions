package com.trickl.flux.websocket;

import com.trickl.exceptions.SubscriptionFailedException;
import com.trickl.flux.publishers.SimpMessagingPublisher;

import java.security.Principal;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Level;

import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;

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
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

@Log
@RequiredArgsConstructor
public class WebSocketRequestRouter<T> implements SmartApplicationListener {

  private final Function<WebSocketRequest, Optional<Flux<T>>> fluxFactory;

  private final SimpUserRegistry simpUserRegistry;

  private final SimpMessagingTemplate messagingTemplate;

  // Destination -> Flux
  private final Map<WebSocketRequest, Optional<Flux<T>>> fluxes = new ConcurrentHashMap<>();

  // SubscriptionId -> Subscription
  private final Map<String, Disposable> subscriptions = new ConcurrentHashMap<>();

  private final WebSocketRequestBuilder webSocketRequestBuilder = new WebSocketRequestBuilder();

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

  protected void subscribe(Principal user, String destination, String subscriptionId) 
      throws SubscriptionFailedException {
    WebSocketRequest webSocketRequest = webSocketRequestBuilder.apply(destination)
        .orElseThrow(() -> new SubscriptionFailedException(
            MessageFormat.format("Destination: {0} not found.", destination)));
    
    Flux<?> flux = this.fluxes.computeIfAbsent(webSocketRequest, fluxFactory::apply)
        .orElseThrow(() -> new SubscriptionFailedException(
            MessageFormat.format("Destination: {0} not found.", destination)));

    SimpMessagingPublisher<?> broadcaster = new SimpMessagingPublisher<>(
        flux, messagingTemplate, destination);

    Disposable subscription = Flux.from(broadcaster.get())
            .doOnCancel(() -> this.fluxes.remove(webSocketRequest))
            .doOnComplete(() -> this.fluxes.remove(webSocketRequest))
            .subscribe();

    this.subscriptions.put(subscriptionId, subscription);
  }

  protected void unsubscribe(String subscriptionId) {
    this.subscriptions.computeIfPresent(subscriptionId,
        (id, subscription) -> {
          subscription.dispose();
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
        this.subscribe(accessor.getUser(), destination, accessor.getSubscriptionId());
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
}
