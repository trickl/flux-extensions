package com.trickl.flux.websocket.sockjs.frames;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trickl.flux.websocket.sockjs.SockJsFrame;
import lombok.Builder;
import lombok.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

@Builder
@Value
public class SockJsMessageFrame implements SockJsFrame {
  protected String message;

  /**
   * Create a SockJsMessageFrame from an existing Message.
   *
   * @param message The message
   * @return A typed message
   */
  public static SockJsFrame from(String message) {
    return SockJsMessageFrame.builder()
        .message(message)
        .build();
  }

  /**
   * Convert to the websocket message.
   *
   * @throws JsonProcessingException if the message cannot be converted
   */
  public Message<String> toMessage() throws JsonProcessingException {
    return MessageBuilder.withPayload(message).build();
  }
}
