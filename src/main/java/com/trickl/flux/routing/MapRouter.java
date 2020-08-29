package com.trickl.flux.routing;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.Builder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class MapRouter<T> {
  private final Function<String, Flux<T>> fluxCreator;

  private final Map<String, Flux<T>> fluxMap = new HashMap<String, Flux<T>>();

  /**
   * Build a new topic flux router.
   */
  @Builder
  public MapRouter(
      Publisher<T> source,
      Function<String, Flux<T>> fluxCreator
  ) {
    this.fluxCreator = Optional.ofNullable(fluxCreator)
        .orElse(name -> Flux.empty());
  }

  /**
   * Get a named flux, creating if if it doesn't exist. 
   *
   * @param name The name.
   * @return A flux for this name
  */
  public Flux<T> get(String name) {
    return fluxMap.computeIfAbsent(name, n -> fluxCreator.apply(n).share());
  }
}