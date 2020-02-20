package com.trickl.flux.websocket.stomp.frames;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trickl.flux.websocket.stomp.StompFrame;
import java.nio.charset.StandardCharsets;
import lombok.Builder;
import lombok.Data;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompConversionException;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;

@Data
@Builder
public class StompMessageFrame implements StompFrame {
  protected String body;
  protected String destination;
  protected String messageId;
  protected String subscriptionId;

  /** Get the stomp headers for this message. */
  public StompHeaderAccessor getHeaderAccessor() {
    StompHeaderAccessor stompHeaderAccessor = StompHeaderAccessor.create(StompCommand.MESSAGE);
    stompHeaderAccessor.setDestination(destination);
    stompHeaderAccessor.setSubscriptionId(subscriptionId);
    stompHeaderAccessor.setMessageId(messageId);
    return stompHeaderAccessor;
  }

  /**
   * Create a StompMessageFrame from an existing Message.
   *
   * @param headerAccessor The header accessor
   * @param payload The message payload
   * @return A typed message
   */
  public static StompFrame from(StompHeaderAccessor headerAccessor, byte[] payload)
      throws StompConversionException {
    String body = new String(payload, StandardCharsets.UTF_8);
    return StompMessageFrame.builder()
        .destination(headerAccessor.getDestination())
        .messageId(headerAccessor.getMessageId())
        .subscriptionId(headerAccessor.getSubscriptionId())
        .body(body)
        .build();
  }

  /**
   * Convert to the websocket message.
   *
   * @throws JsonProcessingException if the message cannot be converted
   */
  public Message<byte[]> toMessage() throws JsonProcessingException {
    return MessageBuilder.createMessage(
        body.getBytes(StandardCharsets.UTF_8), getHeaderAccessor().toMessageHeaders());
  }
}
