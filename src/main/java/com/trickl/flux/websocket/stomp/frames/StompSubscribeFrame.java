package com.trickl.flux.websocket.stomp.frames;

import com.trickl.flux.websocket.stomp.StompFrame;
import lombok.Builder;
import lombok.Data;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;

@Data
@Builder
public class StompSubscribeFrame implements StompFrame {
  protected String destination;
  protected String subscriptionId;

  /** Get the stomp headers for this message. */
  public StompHeaderAccessor getHeaderAccessor() {
    StompHeaderAccessor stompHeaderAccessor = StompHeaderAccessor.create(StompCommand.SUBSCRIBE);
    stompHeaderAccessor.setDestination(destination);
    stompHeaderAccessor.setSubscriptionId(subscriptionId);
    return stompHeaderAccessor;
  }

  /** Convert to the websocket message. */
  public Message<byte[]> toMessage() {
    return MessageBuilder.createMessage(new byte[0], getHeaderAccessor().toMessageHeaders());
  }
}
