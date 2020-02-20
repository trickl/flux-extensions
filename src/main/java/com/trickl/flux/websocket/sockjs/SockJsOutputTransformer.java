package com.trickl.flux.websocket.sockjs;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import org.springframework.web.socket.sockjs.frame.Jackson2SockJsMessageCodec;
import org.springframework.web.socket.sockjs.frame.SockJsFrame;
import org.springframework.web.socket.sockjs.frame.SockJsMessageCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class SockJsOutputTransformer implements Function<Publisher<String>, Flux<String>> {

  private final ObjectMapper objectMapper;

  @Override
  public Flux<String> apply(Publisher<String> source) {
    SockJsMessageCodec codec = new Jackson2SockJsMessageCodec(objectMapper);
    return Flux.from(source).flatMap(payload -> sendFrame(payload, codec));
  }

  protected Publisher<String> sendFrame(String payload, SockJsMessageCodec codec) {

    if (payload == null || payload.isEmpty()) {
      return Mono.empty();
    }

    String content = codec.encode(payload);
    SockJsFrame frame = new SockJsFrame(content);
    return Mono.just(frame.getFrameData());
  }
}
