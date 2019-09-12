package com.trickl.flux.websocket;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DestinationConverter implements Function<String, WebSocketDestination> {

  private static final String TOPIC_PREFIX_PATTERN = "\\/(?<destinationType>topic)";
  private static final String USER_QUEUE_PREFIX_PATTERN =
      "\\/(?<destinationType>user)\\/(?<userName>[a-zA-Z0-9_-]+)";
  private static final String TYPED_STREAM_PATTERN =
      "\\/(?<channelType>[a-zA-Z0-9_-]+)" 
      + "(\\/(?<parameters>[a-zA-Z0-9_-]+(\\/[a-zA-Z0-9_-]+)*))?";

  private final Pattern topicPattern = Pattern.compile(TOPIC_PREFIX_PATTERN + TYPED_STREAM_PATTERN);
  private final Pattern userQueuePattern =
      Pattern.compile(USER_QUEUE_PREFIX_PATTERN + TYPED_STREAM_PATTERN);

  
  @Override
  public WebSocketDestination apply(String destination) {
    Matcher matcher = topicPattern.matcher(destination);

    WebSocketDestination.WebSocketDestinationBuilder builder 
        = WebSocketDestination.builder();

    if (!matcher.matches()) {
      matcher = userQueuePattern.matcher(destination);
      builder.userName(matcher.group("userName"));

      if (!matcher.matches()) {
        return null;
      }
    }

    String channelType = matcher.group("channelType");    
    String parametersString = matcher.group("parameters");
    List<String> parameters = Arrays.asList(parametersString.split("/"));
    
    WebSocketDestinationType destinationType =
        Enum.valueOf(WebSocketDestinationType.class,
        matcher.group("destinationType").toUpperCase());

    return builder.destinationType(destinationType)
        .channelType(channelType)
        .params(parameters)
        .build();
  }
}