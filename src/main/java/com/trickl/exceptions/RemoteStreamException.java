package com.trickl.exceptions;

public class RemoteStreamException extends Exception {

  private static final long serialVersionUID = -1761231643713163261L;

  public RemoteStreamException(String message) {
    super(message);
  }

  public RemoteStreamException(String message, Throwable cause) {
    super(message, cause);
  }

  public RemoteStreamException(Throwable cause) {
    super(cause);
  }
}
