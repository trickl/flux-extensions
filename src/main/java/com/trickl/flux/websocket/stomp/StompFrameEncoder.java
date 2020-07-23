package com.trickl.flux.websocket.stomp;

import com.trickl.flux.mappers.ThrowingFunction;
import java.io.IOException;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompEncoder;

@Log
@RequiredArgsConstructor
public class StompFrameEncoder implements ThrowingFunction<StompFrame, byte[], IOException>  {

  private final StompEncoder encoder = new StompEncoder();

  /**
   * Encode a stomp message into a byte array.
   *
   * @param stompMessage The message to encode
   * @return An array of bytes
   * @throws IOException If the encoding failed
   */
  public byte[] apply(StompFrame stompMessage) throws IOException {
    log.log(Level.FINE, "\u001B[34mSENDING {0}\u001B[0m", new Object[] {stompMessage});
    Message<byte[]> message = stompMessage.toMessage();
    return encoder.encode(message);
  }
}
