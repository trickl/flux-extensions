package com.trickl.flux.mappers;

import java.util.function.Function;
import java.util.logging.Level;
import lombok.extern.java.Log;

@Log
public class WarnOnErrorMapper implements Function<Throwable, Throwable> {

  @Override
  public Throwable apply(Throwable ex) {
    log.log(Level.WARNING, "Unexpected error", ex);
    return ex;
  }
}
