package com.trickl.flux.websocket.stomp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompDecoder;
import org.springframework.messaging.simp.stomp.StompEncoder;

@Log
@RequiredArgsConstructor
public class StompMessageCodec {

  private final StompEncoder encoder = new StompEncoder();
  private final StompDecoder decoder = new StompDecoder();

  /**
   * Encode a stomp message into a byte array.
   *
   * @param stompMessage The message to encode
   * @return An array of bytes
   * @throws IOException If the encoding failed
   */
  public byte[] encode(StompFrame stompMessage) throws IOException {
    log.log(Level.FINE, "\u001B[34mSENDING {0}\u001B[0m", new Object[] {stompMessage});
    Message<byte[]> message = stompMessage.toMessage();
    return encoder.encode(message);
  }

  /**
   * Decode a STOMP message.
   *
   * @param payload The binary payload
   * @return A typed message
   * @throws IOException If the message cannot be decoded
   */
  public List<StompFrame> decode(byte[] payload) throws IOException {
    StompFrameBuilder frameBuilder = new StompFrameBuilder();
    ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
    List<Message<byte[]>> messages = decoder.decode(byteBuffer);
    return messages.stream()
        .map(frameBuilder::apply)
        .map(
            frame -> {
              log.log(Level.FINE, "\u001B[34mRECEIVED {0}\u001B[0m", new Object[] {frame});
              return frame;
            })
        .collect(Collectors.toList());
  }
}
