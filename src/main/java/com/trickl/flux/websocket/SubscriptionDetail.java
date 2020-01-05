package com.trickl.flux.websocket;

import java.time.Instant;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Builder
@Data
@EqualsAndHashCode
public class SubscriptionDetail {
  protected final String subscriptionId;  
  protected final String userName;
  protected final String destination;
  protected final String sessionId;
  protected Instant subscriptionTime;
  protected Instant cancelTime;
  protected Instant completeTime;
  protected Instant errorTime;
  protected String errorMessage;
  protected boolean isCancelled;
}