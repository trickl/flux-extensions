package com.trickl.exceptions;

public class ConnectionFailedException extends Exception {

  private static final long serialVersionUID = -1761231643713163261L;

  public ConnectionFailedException(String message) {
    super(message);
  }

  public ConnectionFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConnectionFailedException(Throwable cause) {
    super(cause);
  }
}
