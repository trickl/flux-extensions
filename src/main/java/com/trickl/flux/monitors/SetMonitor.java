package com.trickl.flux.monitors;

import com.trickl.flux.mappers.ReducingMapper;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.Builder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;

@Builder
public class SetMonitor<T> implements Publisher<SetAction<T>> {

  @Builder.Default
  private final Set<T> initialSet = Collections.emptySet();

  @Builder.Default
  private final Publisher<Set<T>> addPublisher = Flux.empty();

  @Builder.Default
  private final Publisher<Set<T>> removePublisher = Flux.empty();

  @Builder.Default
  private final Publisher<?> clearPublisher = Flux.empty();

  @Override
  public void subscribe(Subscriber<? super SetAction<T>> subscriber) {
    SetAction<T> initialAction = new SetAction<T>(
        SetActionType.Add, Collections.emptySet(), initialSet);

    Flux<SetAction<T>> unaggregatedActions = Flux.merge(
        Flux.from(addPublisher).map(addedSet -> new SetAction<T>(
            SetActionType.Add, addedSet, Collections.emptySet())),
        Flux.from(removePublisher).map(removedSet -> new SetAction<T>(
            SetActionType.Remove, removedSet, Collections.emptySet())),
        Flux.from(clearPublisher).map(signal -> new SetAction<T>(
          SetActionType.Clear, Collections.emptySet(), Collections.emptySet())));

    ReducingMapper<SetAction<T>> reducer 
        = new ReducingMapper<>((
        SetAction<T> last,
        SetAction<T> next) -> {          
          Set<T> aggregateSet = new HashSet<T>(last.getSet());
          if (next.getType().equals(SetActionType.Add)) {
            aggregateSet.addAll(next.getDelta());
          } else if (next.getType().equals(SetActionType.Remove)) {
            aggregateSet.removeAll(next.getDelta());
          } else if (next.getType().equals(SetActionType.Clear)) {
            aggregateSet.clear();
          }

          return new SetAction<T>(
              next.getType(), next.getDelta(), aggregateSet);          
        }, initialAction);

    Flux<SetAction<T>> finalFlux = unaggregatedActions
        .flatMap(reducer);
    finalFlux.subscribe(subscriber);
  }


}
