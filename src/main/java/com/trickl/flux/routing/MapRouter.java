package com.trickl.flux.routing;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import lombok.Builder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class MapRouter<T> {
  private final BiFunction<Publisher<T>, String, Flux<T>> fluxCreator;

  private final Map<String, Flux<T>> fluxMap = new HashMap<String, Flux<T>>();

  /**
   * Build a new topic flux router.
   */
  @Builder
  public MapRouter(
      Publisher<T> source,
      BiFunction<Publisher<T>, String, Flux<T>> fluxCreator
  ) {
    this.fluxCreator = Optional.ofNullable(fluxCreator)
        .orElse((pub, name) -> Flux.from(pub));
  }

  /**
   * Get a named flux, creating if if it doesn't exist. 
   *
   * @param destination The name.
   * @return A flux for this name
  */
  public Flux<T> route(Publisher<T> source, String destination) {
    return fluxMap.computeIfAbsent(destination, name -> fluxCreator.apply(source, name).share());
  }
}