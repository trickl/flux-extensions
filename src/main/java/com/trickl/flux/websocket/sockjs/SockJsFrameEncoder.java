package com.trickl.flux.websocket.sockjs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.mappers.ThrowingFunction;
import com.trickl.flux.websocket.sockjs.frames.SockJsMessageFrame;
import java.io.IOException;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.messaging.Message;
import org.springframework.web.socket.sockjs.frame.Jackson2SockJsMessageCodec;
import org.springframework.web.socket.sockjs.frame.SockJsMessageCodec;

@Log
@RequiredArgsConstructor
public class SockJsFrameEncoder implements ThrowingFunction<SockJsFrame, String, IOException>  {

  private final SockJsMessageCodec encoder;

  public SockJsFrameEncoder(ObjectMapper objectMapper) {
    encoder = new Jackson2SockJsMessageCodec(objectMapper);
  }

  /**
   * Encode a sockJs message into a string.
   *
   * @param sockJsMessage The message to encode
   * @return An array of bytes
   * @throws IOException If the encoding failed
   */
  public String apply(SockJsFrame sockJsMessage) throws IOException {
    log.log(Level.FINE, "\u001B[34mSENDING {0}\u001B[0m", new Object[] {sockJsMessage});
    Message<String> message = sockJsMessage.toMessage();
    if (SockJsMessageFrame.class.equals(sockJsMessage.getClass())) {
      return encoder.encode(message.getPayload());  
    }
    return message.getPayload();
  }
}
