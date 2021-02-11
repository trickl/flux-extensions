package com.trickl.flux.websocket.stomp;

import com.trickl.flux.mappers.ThrowingFunction;
import com.trickl.flux.websocket.stomp.frames.StompHeartbeatFrame;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompDecoder;
import reactor.core.publisher.Flux;

@Log
@RequiredArgsConstructor
public class StompFrameDecoder implements
    ThrowingFunction<byte[], Publisher<StompFrame>, IOException> {

  private final StompDecoder decoder = new StompDecoder();

  /**
   * Decode a STOMP message.
   *
   * @param payload The binary payload
   * @return A typed message
   * @throws IOException If the message cannot be decoded
   */
  public Publisher<StompFrame> apply(byte[] payload) throws IOException {
    StompFrameBuilder frameBuilder = new StompFrameBuilder();
    ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
    List<Message<byte[]>> messages = decoder.decode(byteBuffer);
    return Flux.fromStream(messages.stream()
        .map(frameBuilder::apply)
        .map(
            frame -> {
              if (frame instanceof StompHeartbeatFrame) {
                log.log(Level.FINEST, "\u001B[34mRECEIVED {0}\u001B[0m", new Object[] {frame});
              } else {
                log.log(Level.FINE, "\u001B[34mRECEIVED {0}\u001B[0m", new Object[] {frame});
              }              
              return frame;
            }));
  }
}
