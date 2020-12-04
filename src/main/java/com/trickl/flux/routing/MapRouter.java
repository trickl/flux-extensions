package com.trickl.flux.routing;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import lombok.Builder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class MapRouter<T, DestinationT> {
  private final BiFunction<Publisher<T>, DestinationT, Flux<T>> fluxCreator;

  private final Map<DestinationT, Flux<T>> fluxMap = new HashMap<DestinationT, Flux<T>>();

  /**
   * Build a new topic flux router.
   * 
   * @param source The source publisher.
   * @param fluxCreator The flux generator.
   */
  @Builder
  public MapRouter(
      Publisher<T> source,
      BiFunction<Publisher<T>, DestinationT, Flux<T>> fluxCreator
  ) {
    this.fluxCreator = Optional.ofNullable(fluxCreator)
        .orElse((pub, name) -> Flux.from(pub));
  }

  /**
   * Get a named flux, creating if if it doesn't exist. 
   *
   * @param source The source publisher.
   * @param destination The name.
   * @return A flux for this name
  */
  public Flux<T> route(Publisher<T> source, DestinationT destination) {
    return fluxMap.computeIfAbsent(destination, name -> fluxCreator.apply(source, name)
        .doOnCancel(() -> fluxMap.remove(destination))
        .doOnTerminate(() -> fluxMap.remove(destination))
        .share());
  }
}