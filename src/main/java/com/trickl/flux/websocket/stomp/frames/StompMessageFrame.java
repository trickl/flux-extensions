package com.trickl.flux.websocket.stomp.frames;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.websocket.stomp.StompFrame;

import java.io.IOException;
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
public class StompMessageFrame<T> implements StompFrame {
  protected T value;
  protected String destination;
  protected String messageId;
  protected String subscriptionId;

  
  /**
   * Get the stomp headers for this message.
   */
  public StompHeaderAccessor getHeaderAccessor() {
    StompHeaderAccessor stompHeaderAccessor = StompHeaderAccessor.create(StompCommand.MESSAGE);
    stompHeaderAccessor.setDestination(destination);
    stompHeaderAccessor.setSubscriptionId(subscriptionId);
    stompHeaderAccessor.setMessageId(messageId);
    return stompHeaderAccessor;
  }

  /**
   * Create a StompMessageFrame from an existing Message.
   * @param headerAccessor The header accessor
   * @param payload The message payload
   * @return A typed message
   */
  public static <T> StompFrame create(
      StompHeaderAccessor headerAccessor,
      byte[] payload,
      ObjectMapper objectMapper,
      Class<T> valueType) throws StompConversionException {
    try {
      String content = new String(payload, StandardCharsets.UTF_8);
      T value = objectMapper.readValue(content, valueType);
      return StompMessageFrame.builder()
          .destination(headerAccessor.getDestination())
          .messageId(headerAccessor.getMessageId())
          .subscriptionId(headerAccessor.getSubscriptionId())
          .value(value)
        .build();
    } catch (IOException ex) {
      throw new StompConversionException("Unable to read STOMP message", ex);
    }
  }

  /**
   * Convert to the websocket message.
   * 
   * @throws JsonProcessingException if the message cannot be converted
   */
  public Message<byte[]> toMessage(ObjectMapper objectMapper) throws JsonProcessingException {
    String json = objectMapper.writeValueAsString(value);    
    return MessageBuilder.createMessage(json.getBytes(StandardCharsets.UTF_8), 
        getHeaderAccessor().toMessageHeaders());
  }
}