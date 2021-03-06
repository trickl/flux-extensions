package com.trickl.flux.websocket.sockjs;

import com.trickl.exceptions.AbnormalTerminationException;
import com.trickl.flux.websocket.sockjs.frames.SockJsCloseFrame;
import java.text.MessageFormat;
import java.util.function.Function;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.web.reactive.socket.CloseStatus;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
public class SockJsAbnormalCloseProcessor 
    implements Function<SockJsFrame, Publisher<SockJsFrame>> {

  @Override
  public Publisher<SockJsFrame> apply(SockJsFrame frame) {
    if (SockJsCloseFrame.class.equals(frame.getClass())) {
      SockJsCloseFrame closeMessage = (SockJsCloseFrame) frame;
      CloseStatus closeStatus = closeMessage.getCloseStatus();
      if (closeStatus != CloseStatus.NORMAL) {
        String errorMessage = MessageFormat.format(
            "Unexpected SockJS Close ({0}) - {1}", 
            closeStatus.getCode(), closeStatus.getReason());
        log.warning(errorMessage);

        // Still send the close frame, but send an error message after.
        return Flux.concat(
          Mono.just(frame),
          Mono.error(new AbnormalTerminationException(errorMessage)));
      }
    }
    return Mono.just(frame);   
  }
}
