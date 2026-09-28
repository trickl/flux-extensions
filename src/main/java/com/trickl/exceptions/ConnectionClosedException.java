package com.trickl.exceptions;

/** The remote end closed a connection that was expected to stay open. */
public class ConnectionClosedException extends Exception {

  private static final long serialVersionUID = 4861235590174324529L;

  public ConnectionClosedException(String message) {
    super(message);
  }

  public ConnectionClosedException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConnectionClosedException(Throwable cause) {
    super(cause);
  }
}
