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
public class StompConnectFrame implements StompFrame {
  protected String acceptVersion;
  protected String host;

  /**
   * Get the stomp headers for this message.
   */
  public StompHeaderAccessor getHeaderAccessor() {
    StompHeaderAccessor stompHeaderAccessor = StompHeaderAccessor.create(StompCommand.CONNECT);
    stompHeaderAccessor.setAcceptVersion(acceptVersion);
    stompHeaderAccessor.setHost(host);
    return stompHeaderAccessor;
  }

  /**
   * Convert to the websocket message.
   */
  public Message<byte[]> toMessage() {
    
    return MessageBuilder.createMessage(new byte[0], getHeaderAccessor().toMessageHeaders());
  }
}