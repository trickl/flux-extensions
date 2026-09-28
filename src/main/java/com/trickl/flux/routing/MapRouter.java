package com.trickl.flux.routing;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import lombok.Builder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class MapRouter<T, DestinationT> {
  private final BiFunction<Publisher<T>, DestinationT, Flux<T>> fluxCreator;

  private final Map<DestinationT, Flux<T>> fluxMap = new ConcurrentHashMap<>();

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
    Flux<T> existing = fluxMap.get(destination);
    if (existing != null) {
      return existing;
    }

    // Created outside of the map, as creating a flux may itself route (and so change the map),
    // and each flux only removes its own entry, never a newer one for the same destination
    AtomicReference<Flux<T>> created = new AtomicReference<>();
    created.set(
        fluxCreator
            .apply(source, destination)
            .doOnCancel(() -> fluxMap.remove(destination, created.get()))
            .doOnTerminate(() -> fluxMap.remove(destination, created.get()))
            .share());
    Flux<T> previous = fluxMap.putIfAbsent(destination, created.get());
    return previous != null ? previous : created.get();
  }
}