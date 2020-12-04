package com.trickl.flux.monitors;

import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;

@Value
@EqualsAndHashCode
@ToString
public class SetAction<T> {
  private SetActionType type;
  private Set<T> delta;
  private Set<T> set;
}
