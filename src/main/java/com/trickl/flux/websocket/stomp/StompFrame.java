package com.trickl.flux.websocket.stomp;

import java.io.IOException;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;

public interface StompFrame {
  StompHeaderAccessor getHeaderAccessor();

  Message<byte[]> toMessage() throws IOException;
}
