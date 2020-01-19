package com.trickl.exceptions;

public class NoSuchStreamException extends Exception {

  private static final long serialVersionUID = -1761231643713163261L;

  public NoSuchStreamException(String message) {
    super(message);
  }

  public NoSuchStreamException(String message, Throwable cause) {
    super(message, cause);
  }

  public NoSuchStreamException(Throwable cause) {
    super(cause);
  }
}
