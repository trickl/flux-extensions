package com.trickl.flux.websocket;

import java.util.List;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Singular;

@Builder
@Data
@EqualsAndHashCode
public class StreamId {
  protected final WebSocketDestinationType destinationType;
  protected final String channelType;
  protected final String userName;

  @Singular
  protected final List<String> params;
}