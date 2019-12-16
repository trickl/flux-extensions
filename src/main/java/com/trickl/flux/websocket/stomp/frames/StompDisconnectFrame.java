package com.trickl.flux.websocket.stomp.frames;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.websocket.stomp.StompFrame;

import lombok.Builder;
import lombok.Data;

import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;

@Data
@Builder
public class StompDisconnectFrame implements StompFrame {

  /**
   * Get the stomp headers for this message.
   */
  public StompHeaderAccessor getHeaderAccessor() { 
    return StompHeaderAccessor.create(StompCommand.DISCONNECT);
  }

  /**
   * Convert to the websocket message.
   */
  public Message<byte[]> toMessage(ObjectMapper objectMapper) {
    return MessageBuilder.createMessage(new byte[0], getHeaderAccessor().toMessageHeaders());
  }
}