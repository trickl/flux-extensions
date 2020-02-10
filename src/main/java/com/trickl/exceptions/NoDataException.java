package com.trickl.exceptions;

public class NoDataException extends Exception {

  private static final long serialVersionUID = -1761231643713163261L;

  public NoDataException(String message) {
    super(message);
  }

  public NoDataException(String message, Throwable cause) {
    super(message, cause);
  }

  public NoDataException(Throwable cause) {
    super(cause);
  }
}
