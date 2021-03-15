package com.trickl.flux.websocket.stomp;

import com.trickl.flux.mappers.ThrowingFunction;
import com.trickl.flux.websocket.stomp.frames.StompHeartbeatFrame;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompEncoder;

@Log
@RequiredArgsConstructor
public class StompFrameEncoder
    implements ThrowingFunction<StompFrame, List<byte[]>, IOException>  {

  private final StompEncoder encoder = new StompEncoder();

  /**
   * Encode a stomp message into a byte array.
   *
   * @param stompMessage The message to encode
   * @return An array of bytes
   * @throws IOException If the encoding failed
   */
  public List<byte[]> apply(StompFrame stompMessage) throws IOException {
    if (stompMessage instanceof StompHeartbeatFrame) {
      log.log(Level.FINEST, "\u001B[34mSENDING {0}\u001B[0m", new Object[] {stompMessage});
    } else {
      log.log(Level.FINE, "\u001B[34mSENDING {0}\u001B[0m", new Object[] {stompMessage});
    }
    Message<byte[]> message = stompMessage.toMessage();
    return Collections.singletonList(encoder.encode(message));
  }
}
