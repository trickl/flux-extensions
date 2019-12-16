package com.trickl.flux.websocket.stomp.frames;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.websocket.stomp.StompFrame;

import java.nio.charset.StandardCharsets;

import lombok.Builder;
import lombok.Data;

import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;

@Data
@Builder
public class StompSendFrame<T> implements StompFrame {
  protected T value;
  protected String destination;

  /**
  * Get the stomp headers for this message.
  */
  public StompHeaderAccessor getHeaderAccessor() {
    StompHeaderAccessor stompHeaderAccessor = StompHeaderAccessor.create(StompCommand.SEND);
    stompHeaderAccessor.setDestination(destination);
    return stompHeaderAccessor;
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