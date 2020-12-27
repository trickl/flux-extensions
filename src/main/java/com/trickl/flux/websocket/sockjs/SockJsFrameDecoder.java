package com.trickl.flux.websocket.sockjs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.mappers.ThrowingFunction;
import java.io.IOException;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.web.socket.sockjs.frame.Jackson2SockJsMessageCodec;
import org.springframework.web.socket.sockjs.frame.SockJsFrameType;
import org.springframework.web.socket.sockjs.frame.SockJsMessageCodec;
import reactor.core.publisher.Flux;

@Log
@RequiredArgsConstructor
public class SockJsFrameDecoder 
    implements ThrowingFunction<String, Publisher<SockJsFrame>, IOException> {

  private final SockJsMessageCodec decoder;

  public SockJsFrameDecoder(ObjectMapper objectMapper) {
    decoder = new Jackson2SockJsMessageCodec(objectMapper);
  }

  /**
   * Decode a STOMP message.
   *
   * @param payload The binary payload
   * @return A typed message
   * @throws IOException If the message cannot be decoded
   */
  public Publisher<SockJsFrame> apply(String payload) throws IOException {
    SockJsFrameBuilder frameBuilder = new SockJsFrameBuilder();

    org.springframework.web.socket.sockjs.frame.SockJsFrame spFrame
        = new org.springframework.web.socket.sockjs.frame.SockJsFrame(payload);
    SockJsFrameType frameType = spFrame.getType();
    String[] messages = null;
    if (SockJsFrameType.CLOSE.equals(frameType) || SockJsFrameType.MESSAGE.equals(frameType)) {
      messages = decoder.decode(spFrame.getFrameData());
    }

    return Flux.fromStream(frameBuilder.apply(frameType, messages).stream().map(frame -> {
      log.log(Level.FINE, "\u001B[34mRECEIVED {0}\u001B[0m", new Object[] { frame });
      return frame;
    }));
  }
}
