package com.trickl.flux.websocket;

import java.util.Collection;

import lombok.Builder;
import lombok.Singular;

@Builder
public class WebSocketDestination {
  protected final WebSocketDestinationType destinationType;
  protected final String channelType;
  protected final String userName;

  @Singular
  protected final Collection<String> params;
}