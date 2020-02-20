package com.trickl.flux.websocket.stomp.frames;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trickl.flux.websocket.stomp.StompFrame;
import java.util.Arrays;
import lombok.Data;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;

@Data
public class StompHeartbeatFrame implements StompFrame {

  protected static final byte[] endOfLine = new byte[] {'\n'};

  /** Get the stomp headers for this message. */
  public StompHeaderAccessor getHeaderAccessor() {
    return StompHeaderAccessor.createForHeartbeat();
  }

  /**
   * Convert to the websocket message.
   *
   * @throws JsonProcessingException if the message cannot be converted
   */
  public Message<byte[]> toMessage() throws JsonProcessingException {
    return MessageBuilder.createMessage(endOfLine, getHeaderAccessor().toMessageHeaders());
  }

  public static boolean isHeartbeat(String payload) {
    return Arrays.equals(payload.getBytes(), endOfLine);
  }
}
