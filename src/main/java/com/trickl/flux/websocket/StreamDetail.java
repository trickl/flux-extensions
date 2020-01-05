package com.trickl.flux.websocket;

import java.time.Instant;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Builder
@Data
@EqualsAndHashCode
public class StreamDetail {
  protected StreamId id; 
  protected int subscriberCount;
  protected int messageCount;
  protected Instant subscriptionTime;
  protected Instant lastMessageTime;
  protected Instant cancelTime;
  protected Instant completeTime;
  protected Instant errorTime;
  protected String errorMessage;
  protected boolean isTerminated;
}