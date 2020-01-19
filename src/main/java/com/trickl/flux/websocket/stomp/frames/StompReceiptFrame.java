package com.trickl.flux.websocket.stomp.frames;

import com.trickl.flux.websocket.stomp.StompFrame;

import lombok.Builder;
import lombok.Data;

import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;

@Data
@Builder
public class StompReceiptFrame implements StompFrame {
  protected String receiptId;

  /**
   * Get the stomp headers for this message.
   */
  public StompHeaderAccessor getHeaderAccessor() {
    StompHeaderAccessor stompHeaderAccessor = StompHeaderAccessor.create(StompCommand.RECEIPT);
    stompHeaderAccessor.setReceiptId(receiptId);
    return stompHeaderAccessor;
  }

  /**
   * Create a StompReceiptFrame from an existing Message.
   * @param headerAccessor The header accessor
   * @return A typed message
   */
  public static StompFrame from(StompHeaderAccessor headerAccessor) {
    return StompReceiptFrame.builder()
        .receiptId(headerAccessor.getReceiptId())
      .build();
  }

  /**
   * Convert to the websocket message.
   */
  public Message<byte[]> toMessage() {
    return MessageBuilder.createMessage(new byte[0], getHeaderAccessor().toMessageHeaders());
  }
}