package com.trickl.flux.websocket;

import com.trickl.model.streams.StreamId;
import com.trickl.model.streams.StreamType;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamIdParser implements Function<String, Optional<StreamId>> {

  private static final String TOPIC_PREFIX_PATTERN = "\\/(?<streamType>topic)";
  private static final String USER_QUEUE_PREFIX_PATTERN =
      "\\/(?<streamType>user)\\/(?<userName>[a-zA-Z0-9_-]+)";
  private static final String TYPED_STREAM_PATTERN =
      "\\/(?<channel>[a-zA-Z0-9_-]+)" + "(\\/(?<parameters>[a-zA-Z0-9_-]+(\\/[a-zA-Z0-9_-]+)*))?";

  private final Pattern topicPattern = Pattern.compile(TOPIC_PREFIX_PATTERN + TYPED_STREAM_PATTERN);
  private final Pattern userQueuePattern =
      Pattern.compile(USER_QUEUE_PREFIX_PATTERN + TYPED_STREAM_PATTERN);

  @Override
  public Optional<StreamId> apply(String destination) {
    Matcher matcher = topicPattern.matcher(destination);

    StreamId.StreamIdBuilder builder = StreamId.builder();

    if (!matcher.matches()) {
      matcher = userQueuePattern.matcher(destination);

      if (!matcher.matches()) {
        return Optional.empty();
      }

      builder.userName(matcher.group("userName"));
    }

    String channel = matcher.group("channel").toUpperCase();
    String parametersString = matcher.group("parameters");
    List<String> parameters = Arrays.asList(parametersString.split("/"));

    StreamType streamType =
        Enum.valueOf(StreamType.class, matcher.group("streamType").toUpperCase());

    return Optional.of(builder.type(streamType).channel(channel).parameters(parameters).build());
  }
}
