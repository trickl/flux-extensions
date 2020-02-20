package com.trickl.collections;

import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

public class ConditionalDelegatingFactory<T, S> {
  private static final int INITIAL_CAPACITY = 4;
  PriorityQueue<DelegateFunctionWithPriority> delegates =
      new PriorityQueue<>(
          INITIAL_CAPACITY, (a, b) -> Integer.compare(a.getPriority(), b.getPriority()));

  /**
   * Add a factory to delegate to.
   *
   * @param function The factory to delegate to.
   * @param predicate The condition to satisfy to use this delegate
   */
  public void addFunction(Function<T, S> function, Predicate<T> predicate) {
    Optional<Integer> lowestPriority =
        delegates.stream().map(DelegateFunctionWithPriority::getPriority).reduce(Math::max);
    addFunction(function, predicate, lowestPriority.orElse(0) + 1);
  }

  public void addFunction(Function<T, S> function, Predicate<T> predicate, int priority) {
    removeFunction(function);
    delegates.add(new DelegateFunctionWithPriority(function, predicate, priority));
  }

  public boolean removeFunction(Function<T, S> function) {
    return delegates.removeIf(delegate -> delegate.getFunction().equals(function));
  }

  /**
   * Apply the first function whose predicate succeeds the test value.
   *
   * @param value The value to check against each predicate
   * @return The result of applying the first matching function.
   */
  public Optional<S> applyFirstMatch(T value) {
    return delegates.stream()
        .filter(delegate -> delegate.getPredicate().test(value))
        .findFirst()
        .map(delegate -> delegate.getFunction().apply(value));
  }

  @ToString
  @Value
  @EqualsAndHashCode
  private class DelegateFunctionWithPriority {
    protected final Function<T, S> function;

    protected final Predicate<T> predicate;

    protected final int priority;
  }
}
