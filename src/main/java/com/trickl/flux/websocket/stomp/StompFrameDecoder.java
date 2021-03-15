package com.trickl.flux.websocket.stomp;

import com.trickl.flux.mappers.ThrowingFunction;
import com.trickl.flux.websocket.stomp.frames.StompHeartbeatFrame;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompDecoder;

@Log
@RequiredArgsConstructor
public class StompFrameDecoder implements
    ThrowingFunction<byte[], List<StompFrame>, IOException> {

  private final StompDecoder decoder = new StompDecoder();

  /**
   * Decode a STOMP message.
   *
   * @param payload The binary payload
   * @return A typed message
   * @throws IOException If the message cannot be decoded
   */
  public List<StompFrame> apply(byte[] payload) throws IOException {
    StompFrameBuilder frameBuilder = new StompFrameBuilder();
    ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
    List<Message<byte[]>> messages = decoder.decode(byteBuffer);
    return messages.stream()
        .map(frameBuilder::apply)
        .map(
            frame -> {
              if (frame instanceof StompHeartbeatFrame) {
                log.log(Level.FINEST, "\u001B[34mRECEIVED {0}\u001B[0m", new Object[] {frame});
              } else {
                log.log(Level.FINE, "\u001B[34mRECEIVED {0}\u001B[0m", new Object[] {frame});
              }              
              return frame;
            })
          .collect(Collectors.toList());
  }
}
