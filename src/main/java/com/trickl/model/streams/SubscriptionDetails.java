package com.trickl.model.streams;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import javax.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Builder
@Data
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SubscriptionDetails {
  @Nonnull protected String sessionId;
  @Nonnull protected String id;
  @Nonnull protected String destination;

  protected int subscriberCount;
  protected int messageCount;
  protected Instant connectionTime;
  protected Instant subscriptionTime;
  protected Instant lastMessageTime;
  protected Instant cancelTime;
  protected Instant completeTime;
  protected Instant unsubscribeTime;
  protected Instant errorTime;
  protected String errorMessage;
  protected boolean isTerminated;
}
