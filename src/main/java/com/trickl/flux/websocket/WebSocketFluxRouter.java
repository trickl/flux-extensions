package com.trickl.flux.websocket;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.event.SmartApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
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

@RequiredArgsConstructor
public class WebSocketFluxRouter implements SmartApplicationListener {

  private final Function<String, Flux<?>> fluxFactory;

  private final SimpUserRegistry simpUserRegistry;

  // Destination -> Flux
  private final Map<String, Flux<?>> fluxes = new ConcurrentHashMap<>();

  // SubscriptionId -> Subscription
  private final Map<String, Disposable> subscriptions = new ConcurrentHashMap<>();

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

  protected void subscribe(String destination, String subscriptionId) {
    Flux<?> flux = this.fluxes.computeIfAbsent(destination, fluxFactory::apply);

    Disposable subscription =
        flux.doOnCancel(() -> this.fluxes.remove(destination))
            .doOnComplete(() -> this.fluxes.remove(destination))
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
    AbstractSubProtocolEvent subProtocolEvent = (AbstractSubProtocolEvent) event;
    Message<?> message = subProtocolEvent.getMessage();

    SimpMessageHeaderAccessor accessor =
        MessageHeaderAccessor.getAccessor(message, SimpMessageHeaderAccessor.class);
    Assert.state(accessor != null, "No SimpMessageHeaderAccessor");

    String destination = accessor.getDestination();
    Assert.state(destination != null, "No destination");

    if (event instanceof SessionSubscribeEvent) {
      this.subscribe(destination, accessor.getSubscriptionId());
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
