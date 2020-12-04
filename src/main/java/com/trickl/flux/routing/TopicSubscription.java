package com.trickl.flux.routing;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode
public class TopicSubscription<TopicT> {
  private int id;
  private TopicT topic;
}