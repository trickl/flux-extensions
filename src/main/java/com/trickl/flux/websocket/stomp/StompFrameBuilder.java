package com.trickl.flux.websocket.stomp;

import com.trickl.flux.websocket.stomp.StompFrame;
import com.trickl.flux.websocket.stomp.frames.StompConnectedFrame;
import com.trickl.flux.websocket.stomp.frames.StompErrorFrame;
import com.trickl.flux.websocket.stomp.frames.StompMessageFrame;
import com.trickl.flux.websocket.stomp.frames.StompReceiptFrame;

import java.util.function.Function;

import lombok.RequiredArgsConstructor;

import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompConversionException;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;

@RequiredArgsConstructor
public class StompFrameBuilder implements Function<Message<byte[]>, StompFrame> {

  /**
   * Build a stomp frame from a websocket message.
   * @param message The websocket message
   * @return A stomp frame
   * @throws StompConversionException If 
   */
  @Override
  public StompFrame apply(Message<byte[]> message) {
    StompHeaderAccessor headerAccessor = StompHeaderAccessor.wrap(message);
    switch (headerAccessor.getCommand()) {
      case MESSAGE:
        return StompMessageFrame.create(
            headerAccessor, message.getPayload());
      case CONNECTED:
        return StompConnectedFrame.create(headerAccessor);
      case RECEIPT:
        return StompReceiptFrame.create(headerAccessor);
      case ERROR:
        return StompErrorFrame.create(headerAccessor);
      default:
        throw new StompConversionException("Unable to decode STOMP message" 
            + headerAccessor.toMap());
    }
  }
}