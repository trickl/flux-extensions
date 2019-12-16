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
public class StompConnectedFrame implements StompFrame {
  protected String version;

  /**
   * Get the stomp headers for this message.
   */
  public StompHeaderAccessor getHeaderAccessor() {
    StompHeaderAccessor stompHeaderAccessor = StompHeaderAccessor.create(StompCommand.CONNECTED);
    stompHeaderAccessor.setVersion(version);
    return stompHeaderAccessor;
  }

  /**
   * Create a StompConnectedFrame from an existing Message.
   * @param headerAccessor The header accessor
   * @return A typed message
   */
  public static StompFrame create(StompHeaderAccessor headerAccessor) {
    return StompConnectedFrame.builder()
        .version(headerAccessor.getVersion())
      .build();
  }

  /**
   * Convert to the websocket message.
   */
  public Message<byte[]> toMessage(ObjectMapper objectMapper) {
    
    return MessageBuilder.createMessage(new byte[0], getHeaderAccessor().toMessageHeaders());
  }
}