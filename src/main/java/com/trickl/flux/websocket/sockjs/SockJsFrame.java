package com.trickl.flux.websocket.sockjs;

import java.io.IOException;
import org.springframework.messaging.Message;

/** Any response received on the stream. */
public interface SockJsFrame {
  Message<String> toMessage() throws IOException;
}
