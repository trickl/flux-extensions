package com.trickl.flux.monitors;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.test.StepVerifier;

@ActiveProfiles({"unittest"})
public class SetMonitorTest {
  @Test
  public void testAdd() {
    SetMonitor<Integer> setMonitor = SetMonitor.<Integer>builder()
        .addPublisher(Flux.just(1, 2, 3, 4, 5).map(value -> Collections.singleton(value)))
        .build();

    StepVerifier.create(setMonitor)
        .expectNext(new SetAction<>(SetActionType.Add, Set.of(1), Set.of(1)))
        .expectNext(new SetAction<>(SetActionType.Add, Set.of(2), Set.of(1, 2)))
        .expectNext(new SetAction<>(SetActionType.Add, Set.of(3), Set.of(1, 2, 3)))
        .expectNext(new SetAction<>(SetActionType.Add, Set.of(4), Set.of(1, 2, 3, 4)))
        .expectNext(new SetAction<>(SetActionType.Add, Set.of(5), Set.of(1, 2, 3, 4, 5)))
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }

  @Test
  public void testRemove() {
    SetMonitor<Integer> setMonitor = SetMonitor.<Integer>builder()
        .initialSet(Set.of(1, 2, 3, 4, 5))
        .removePublisher(Flux.just(5, 4, 3, 2, 1).map(value -> Collections.singleton(value)))
        .build();

    StepVerifier.create(setMonitor)
        .expectNext(new SetAction<>(SetActionType.Remove, Set.of(5), Set.of(1, 2, 3, 4)))
        .expectNext(new SetAction<>(SetActionType.Remove, Set.of(4), Set.of(1, 2, 3)))
        .expectNext(new SetAction<>(SetActionType.Remove, Set.of(3), Set.of(1, 2)))
        .expectNext(new SetAction<>(SetActionType.Remove, Set.of(2), Set.of(1)))
        .expectNext(new SetAction<>(SetActionType.Remove, Set.of(1), Collections.emptySet()))
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }

  @Test
  public void testCombineAddAndRemove() {
    EmitterProcessor<Integer> addProcessor = EmitterProcessor.<Integer>create();
    EmitterProcessor<Integer> removeProcessor = EmitterProcessor.<Integer>create();
    FluxSink<Integer> addSink = addProcessor.sink();
    FluxSink<Integer> removeSink = removeProcessor.sink();
    
    SetMonitor<Integer> setMonitor = SetMonitor.<Integer>builder()
        .addPublisher(addProcessor.map(value -> Collections.singleton(value)))
        .removePublisher(removeProcessor.map(value -> Collections.singleton(value)))
        .build();

    StepVerifier.create(setMonitor)
        .expectSubscription()
        .expectNoEvent(Duration.ofMillis(100))
        .then(() -> { 
          addSink.next(1);
        })
        .expectNext(new SetAction<>(SetActionType.Add, Set.of(1), Set.of(1)))
        .then(() -> {
          addSink.next(2);
        })
        .expectNext(new SetAction<>(SetActionType.Add, Set.of(2), Set.of(1, 2)))
        .then(() -> {
          addSink.next(3);
        })
        .expectNext(new SetAction<>(SetActionType.Add, Set.of(3), Set.of(1, 2, 3)))
        .then(() -> {
          removeSink.next(3);
        })
        .expectNext(new SetAction<>(SetActionType.Remove, Set.of(3), Set.of(1, 2)))
        .then(() -> {
          removeSink.next(1);
        })
        .expectNext(new SetAction<>(SetActionType.Remove, Set.of(1), Set.of(2)))
        .then(() -> {
          addSink.next(4);
        })
        .expectNext(new SetAction<>(SetActionType.Add, Set.of(4), Set.of(2, 4)))
        .then(() -> {
          removeSink.next(2);
        })
        .expectNext(new SetAction<>(SetActionType.Remove, Set.of(2), Set.of(4)))
        .then(() -> {
          removeSink.next(4);
        })
        .expectNext(new SetAction<>(SetActionType.Remove, Set.of(4), Collections.emptySet()))
        .then(() -> {
          addSink.complete();
          removeSink.complete();
        })
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }
}