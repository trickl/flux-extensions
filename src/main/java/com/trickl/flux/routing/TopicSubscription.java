package com.trickl.flux.routing;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode
public class TopicSubscription {
  private int id;
  private String topic;
}