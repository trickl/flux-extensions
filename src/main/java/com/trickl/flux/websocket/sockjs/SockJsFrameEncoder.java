package com.trickl.flux.websocket.sockjs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trickl.flux.mappers.ThrowingFunction;
import com.trickl.flux.websocket.sockjs.frames.SockJsMessageFrame;
import java.io.IOException;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.messaging.Message;
import org.springframework.web.socket.sockjs.frame.Jackson2SockJsMessageCodec;
import org.springframework.web.socket.sockjs.frame.SockJsMessageCodec;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class SockJsFrameEncoder
    implements ThrowingFunction<SockJsFrame, Publisher<String>, IOException> {

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
  public Publisher<String> apply(SockJsFrame sockJsMessage) throws IOException {
    log.log(Level.FINE, "\u001B[34mSENDING {0}\u001B[0m", new Object[] {sockJsMessage});
    Message<String> message = sockJsMessage.toMessage();
    if (SockJsMessageFrame.class.equals(sockJsMessage.getClass())) {     
      // Upstream messages are just a string or array of strings,
      // which is the SockJsMessageFrame encoding without the "a" prefix 
      return Mono.just(encoder.encode(message.getPayload()).substring(1));
      /*
      return Mono.just("[\"{\\\"user_type\\\":\\\"web-desktop\\\",\\\"logged_in\\\":true,"
       + "\\\"add\\\":[{\\\"names\\\":[\\\"event*\\\"],\\\"ids\\\":[41988657]},{\\\"names\\\""
       + ":[\\\"q\\\"]"
       + ",\\\"ids\\\":[11946748]},{\\\"names\\\":[\\\"markets/execution\\\"],\\\"ids\\\""
       + ":[11946748]},"
       + "{\\\"names\\\":[\\\"markets/state\\\"],\\\"ids\\\":[11946748]}],\\\"bet_permission\\\""
       + ":true}\"]");
       */
    }
    return Mono.empty();
  }
}
