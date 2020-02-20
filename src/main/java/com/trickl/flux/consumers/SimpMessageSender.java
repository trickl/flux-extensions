package com.trickl.flux.consumers;

import java.util.function.Consumer;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.springframework.messaging.simp.SimpMessagingTemplate;

@Log
@RequiredArgsConstructor
public class SimpMessageSender<T> implements Consumer<T> {

  private final SimpMessagingTemplate messagingTemplate;
  private final String destination;

  @Override
  public void accept(T t) {
    sendMessage(t);
  }

  void sendMessage(T value) {
    log.log(
        Level.FINE, "\u001B[32mSENDING  ↑ {0}\u001B[0m on {1}", new Object[] {value, destination});
    messagingTemplate.convertAndSend(destination, value);
  }
}
