package com.trickl.flux.websocket.sockjs.frames;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trickl.flux.websocket.sockjs.SockJsFrame;
import lombok.Builder;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

@Builder
public class SockJsOpenFrame implements SockJsFrame {
  /**
  * Convert to the websocket message.
  *
  * @throws JsonProcessingException if the message cannot be converted
  */
  public Message<String> toMessage() throws JsonProcessingException {
    return MessageBuilder.withPayload("o").build();
  }
}
