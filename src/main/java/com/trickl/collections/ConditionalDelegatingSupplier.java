package com.trickl.collections;

import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

public class ConditionalDelegatingSupplier<T, S>  {
  private static final int INITIAL_CAPACITY = 4;
  PriorityQueue<DelegateSupplierWithPriority> delegates = 
      new PriorityQueue<>(INITIAL_CAPACITY, (a, b) ->
        Integer.compare(a.getPriority(), b.getPriority())
      );

  /**
   * Add a factory to delegate to.
   *
   * @param supplier The factory to delegate to.
   * @param predicate The condition to satisfy to use this delegate
   */
  public void addSupplier(Supplier<S> supplier, Predicate<T> predicate) {
    Optional<Integer> lowestPriority =
        delegates.stream().map(DelegateSupplierWithPriority::getPriority).reduce(Math::max);
    addSupplier(supplier, predicate, lowestPriority.orElse(0) + 1);
  }

  public void addSupplier(Supplier<S> supplier, Predicate<T> predicate, int priority) {
    removeSupplier(supplier);
    delegates.add(new DelegateSupplierWithPriority(supplier, predicate, priority));
  }

  public boolean removeSupplier(Supplier<S> supplier) {
    return delegates.removeIf(delegate -> delegate.getSupplier().equals(supplier));
  }

  /**
   * Apply all suppliers.
   */
  public List<S> applyAll() {
    return delegates.stream()
        .map(delegate -> delegate.getSupplier().get())
        .collect(Collectors.toList());
  }

  /**
   * Apply the first supplier whose predicate succeeds the test value.
   * @param testValue The value to check against each predicate
   * @return The result of applying the first matching supplier.
   */
  public Optional<S> applyFirstMatch(T testValue) {
    return delegates.stream()
        .filter(delegate -> delegate.getPredicate().test(testValue))
        .findFirst()
        .map(delegate -> delegate.getSupplier().get());
  }

  @ToString
  @Value
  @EqualsAndHashCode
  private class DelegateSupplierWithPriority {
    protected final Supplier<S> supplier;

    protected final Predicate<T> predicate;

    protected final int priority;
  }
}