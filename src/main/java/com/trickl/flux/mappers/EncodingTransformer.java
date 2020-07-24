package com.trickl.flux.mappers;

import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RequiredArgsConstructor
public class EncodingTransformer<T, S> implements Function<Publisher<T>, Flux<S>> {

  private final ThrowingFunction<T, S, ? extends Exception> encoder;

  @Override
  public Flux<S> apply(Publisher<T> source) {
    ThrowableMapper<T, S> mapper = 
        new ThrowableMapper<T, S>(encoder);
    return Flux.from(source).flatMap(frame -> {
      return frame == null ? Mono.empty() : mapper.apply(frame);
    });
  }
}