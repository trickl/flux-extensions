package com.trickl.flux.publishers;

import java.util.function.Supplier;
import java.util.logging.Level;
import lombok.extern.java.Log;
import org.reactivestreams.Publisher;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import reactor.core.publisher.Flux;

@Log
public class MessageTopicBroadcaster<T> implements Supplier<Publisher<T>> {

  private final Publisher<T> source;
  private final SimpMessagingTemplate messagingTemplate;
  private final String destination;
  private final Publisher<T> publisher;

  /**
   * Create a message topic broadcaster.
   *
   * @param source The underlying data source
   * @param messagingTemplate Messaging template for broadcast
   * @param destination The destination of messages
   */
  public MessageTopicBroadcaster(
      Publisher<T> source, SimpMessagingTemplate messagingTemplate, String destination) {
    this.source = source;
    this.messagingTemplate = messagingTemplate;
    this.destination = destination;

    publisher = Flux.from(this.source)    
      .doOnNext(this::sendMessage)
      .publish()
      .refCount();     
  }

  @Override
  public Publisher<T> get() {
    return publisher;
  }

  void sendMessage(T value) {  
    log.log(
        Level.FINE, "\u001B[32mSENDING  ↑ {0}\u001B[0m on {1}",
        new Object[] {value, destination});
    messagingTemplate.convertAndSend(destination, value);
  
  }
}