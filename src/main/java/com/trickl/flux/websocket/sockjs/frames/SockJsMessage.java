package com.trickl.flux.websocket.sockjs.frames;

import lombok.Value;

@Value
public class SockJsMessage implements SockJsFrame  {
  protected String payload;
}
