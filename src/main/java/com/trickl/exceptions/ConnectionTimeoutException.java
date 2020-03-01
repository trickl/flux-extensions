package com.trickl.exceptions;

public class ConnectionTimeoutException extends Exception {

  private static final long serialVersionUID = -1761231643713163261L;

  public ConnectionTimeoutException(String message) {
    super(message);
  }

  public ConnectionTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

  public ConnectionTimeoutException(Throwable cause) {
    super(cause);
  }
}
