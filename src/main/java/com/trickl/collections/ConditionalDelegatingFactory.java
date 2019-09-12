package com.trickl.collections;

import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.function.Predicate;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

public class ConditionalDelegatingFactory<T, S> implements Function<T, Optional<S>> {
  private static final int INITIAL_CAPACITY = 4;
  PriorityQueue<DelegateFactoryWithPriority> delegates = 
      new PriorityQueue<>(INITIAL_CAPACITY, (a, b) ->
        Integer.compare(a.getPriority(), b.getPriority())
      );

  /**
   * Add a factory to delegate to.
   *
   * @param factory The factory to delegate to.
   * @param predicate The condition to satisfy to use this delegate
   */
  public void addFactory(Function<T, S> factory, Predicate<T> predicate) {
    Optional<Integer> lowestPriority =
        delegates.stream().map(DelegateFactoryWithPriority::getPriority).reduce(Math::max);
    addFactory(factory, predicate, lowestPriority.orElse(0) + 1);
  }

  public void addFactory(Function<T, S> factory, Predicate<T> predicate, int priority) {
    removeFactory(factory);
    delegates.add(new DelegateFactoryWithPriority(factory, predicate, priority));
  }

  public boolean removeFactory(Function<T, S> factory) {
    return delegates.removeIf(delegate -> delegate.getFactory().equals(factory));
  }

  @Override
  public Optional<S> apply(T params) {
    return delegates.stream()
        .filter(delegate -> delegate.getPredicate().test(params))
        .findFirst()
        .map(delegate -> delegate.getFactory().apply(params));
  }

  @ToString
  @Value
  @EqualsAndHashCode
  private class DelegateFactoryWithPriority {
    protected final Function<T, S> factory;

    protected final Predicate<T> predicate;

    protected final int priority;
  }
}