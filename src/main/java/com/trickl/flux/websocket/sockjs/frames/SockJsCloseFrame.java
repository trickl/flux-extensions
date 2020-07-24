package com.trickl.flux.websocket.sockjs.frames;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.trickl.flux.websocket.sockjs.SockJsFrame;
import lombok.Builder;
import lombok.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.reactive.socket.CloseStatus;

@Builder
@Value
public class SockJsCloseFrame implements SockJsFrame {
  protected final CloseStatus closeStatus;

  /**
  * Create a SockJsMessageFrame from an existing Message.
  *
  * @param data The close status
  * @return A typed message
  */
  public static SockJsFrame from(String[] data) {    
    return SockJsCloseFrame.builder()
        .closeStatus(new CloseStatus(Integer.parseInt(data[0]), data.length > 1 ? data[1] : null))
        .build();
  }

  /**
  * Convert to the websocket message.
  *
  * @throws JsonProcessingException if the message cannot be converted
  */
  public Message<String> toMessage() throws JsonProcessingException {
    return MessageBuilder.withPayload("c[" + closeStatus.getCode() + ",\"" 
        + (closeStatus.getReason() != null ? closeStatus.getReason()  : "") + "\"]").build();
  }
}
