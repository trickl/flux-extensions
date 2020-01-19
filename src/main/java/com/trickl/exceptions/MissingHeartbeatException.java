package com.trickl.exceptions;

public class MissingHeartbeatException extends Exception {

  private static final long serialVersionUID = -1761231643713163261L;

  public MissingHeartbeatException(String message) {
    super(message);
  }

  public MissingHeartbeatException(String message, Throwable cause) {
    super(message, cause);
  }

  public MissingHeartbeatException(Throwable cause) {
    super(cause);
  }
}
