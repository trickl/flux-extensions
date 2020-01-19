package com.trickl.exceptions;

public class SubscriptionFailedException extends Exception {

  private static final long serialVersionUID = -1761231643713163261L;

  public SubscriptionFailedException(String message) {
    super(message);
  }

  public SubscriptionFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public SubscriptionFailedException(Throwable cause) {
    super(cause);
  }
}
