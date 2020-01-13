package com.trickl.model.streams;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;

public enum StreamType {
  @JsonPropertyDescription("Broadcast to all listening")
  TOPIC, 
  @JsonPropertyDescription("Only sent once, possibly to different users")
  QUEUE,
  @JsonPropertyDescription("Broadcast to all subscriptions for a particular user")
  USER,
  @JsonPropertyDescription("Only sent to a particular user subscription")
  SUBSCRIPTION, 
}