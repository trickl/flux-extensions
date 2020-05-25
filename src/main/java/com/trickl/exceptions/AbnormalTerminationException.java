package com.trickl.exceptions;

public class AbnormalTerminationException extends Exception {

  private static final long serialVersionUID = -1761231643713163261L;

  public AbnormalTerminationException(String message) {
    super(message);
  }

  public AbnormalTerminationException(String message, Throwable cause) {
    super(message, cause);
  }

  public AbnormalTerminationException(Throwable cause) {
    super(cause);
  }
}
