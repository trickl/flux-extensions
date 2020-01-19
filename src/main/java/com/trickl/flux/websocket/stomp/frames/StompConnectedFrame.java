package com.trickl.flux.websocket.stomp.frames;

import com.trickl.flux.websocket.stomp.StompFrame;

import java.time.Duration;

import lombok.Builder;
import lombok.Data;

import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;

@Data
@Builder
public class StompConnectedFrame implements StompFrame {
  protected String version;

  @Builder.Default
  protected Duration heartbeatSendFrequency = Duration.ZERO;

  @Builder.Default
  protected Duration heartbeatReceiveFrequency = Duration.ZERO;

  /**
   * Get the stomp headers for this message.
   */
  public StompHeaderAccessor getHeaderAccessor() {
    StompHeaderAccessor stompHeaderAccessor = StompHeaderAccessor.create(StompCommand.CONNECTED);
    stompHeaderAccessor.setVersion(version);
    stompHeaderAccessor.setHeartbeat(
        heartbeatSendFrequency.toMillis(),
        heartbeatReceiveFrequency.toMillis());
    return stompHeaderAccessor;
  }

  /**
   * Create a StompConnectedFrame from an existing Message.
   * @param headerAccessor The header accessor
   * @return A typed message
   */
  public static StompFrame from(StompHeaderAccessor headerAccessor) {
    long heartbeatSendFrequency = 0;
    long heartbeatReceiveFrequency = 0;
    long[] heartbeat = headerAccessor.getHeartbeat();
    if (heartbeat != null) {
      if (heartbeat.length != 0) {
        heartbeatSendFrequency = heartbeat[0];
      }
      if (heartbeat.length > 1) {
        heartbeatReceiveFrequency = heartbeat[1];
      } 
    }
    return StompConnectedFrame.builder()
        .version(headerAccessor.getVersion())
        .heartbeatSendFrequency(Duration.ofMillis(heartbeatSendFrequency))
        .heartbeatReceiveFrequency(Duration.ofMillis(heartbeatReceiveFrequency))
      .build();
  }

  /**
   * Convert to the websocket message.
   */
  public Message<byte[]> toMessage() {    
    return MessageBuilder.createMessage(new byte[0], getHeaderAccessor().toMessageHeaders());
  }
}