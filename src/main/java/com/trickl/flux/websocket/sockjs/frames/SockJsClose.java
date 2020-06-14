package com.trickl.flux.websocket.sockjs.frames;

import lombok.Value;
import org.springframework.web.reactive.socket.CloseStatus;

@Value
public class SockJsClose implements SockJsFrame {
  protected CloseStatus status;
}
