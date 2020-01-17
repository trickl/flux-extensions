package com.trickl.flux.websocket.sockjs.frames;

import lombok.Value;

@Value
public class SockJsClose implements SockJsFrame {
  protected int status;
}
