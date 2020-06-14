package com.trickl.exceptions;

public class ReceiptTimeoutException extends Exception {

  private static final long serialVersionUID = -1761231643713163261L;

  public ReceiptTimeoutException(String message) {
    super(message);
  }

  public ReceiptTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

  public ReceiptTimeoutException(Throwable cause) {
    super(cause);
  }
}
