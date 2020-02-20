package com.trickl.flux.websocket.stomp.frames;

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
public class StompSendFrame implements StompFrame {
  protected String body;
  protected String destination;

  /** Get the stomp headers for this message. */
  public StompHeaderAccessor getHeaderAccessor() {
    StompHeaderAccessor stompHeaderAccessor = StompHeaderAccessor.create(StompCommand.SEND);
    stompHeaderAccessor.setDestination(destination);
    return stompHeaderAccessor;
  }

  /** Convert to the websocket message. */
  public Message<byte[]> toMessage() {
    return MessageBuilder.createMessage(
        body.getBytes(StandardCharsets.UTF_8), getHeaderAccessor().toMessageHeaders());
  }
}
