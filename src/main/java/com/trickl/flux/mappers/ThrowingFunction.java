package com.trickl.flux.mappers;

@FunctionalInterface
public interface ThrowingFunction<T, S, E extends Exception> {
  S apply(T t) throws E;
}