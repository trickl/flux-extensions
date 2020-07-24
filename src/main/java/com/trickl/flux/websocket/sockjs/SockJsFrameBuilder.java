package com.trickl.flux.websocket.sockjs;

import com.trickl.flux.websocket.sockjs.frames.SockJsCloseFrame;
import com.trickl.flux.websocket.sockjs.frames.SockJsHeartbeatFrame;
import com.trickl.flux.websocket.sockjs.frames.SockJsMessageFrame;
import com.trickl.flux.websocket.sockjs.frames.SockJsOpenFrame;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.web.socket.sockjs.frame.SockJsFrameType;

@RequiredArgsConstructor
public class SockJsFrameBuilder
    implements BiFunction<SockJsFrameType, String[], List<SockJsFrame>> {

  /**
  * Build a stomp frame from a websocket message.
  *
  * @param messages The websocket messages
  * @return A stomp frame
  * @throws SockJsConversionException If
  */
  @Override
  public List<SockJsFrame> apply(SockJsFrameType frameType, String[] messages) {
    switch (frameType) {
      case OPEN:
        return Arrays.asList(SockJsOpenFrame.builder().build());
      case HEARTBEAT:
        return Arrays.asList(SockJsHeartbeatFrame.builder().build());
      case MESSAGE:
        return Arrays.asList(messages).stream()
        .map(message -> SockJsMessageFrame.from(message)).collect(Collectors.toList());
      case CLOSE:
        return Arrays.asList(SockJsCloseFrame.from(messages));
      default:
        throw new SockJsConversionException(
            "Unable to decode SockJS message " + frameType + " " + messages);
    }
  }
}
