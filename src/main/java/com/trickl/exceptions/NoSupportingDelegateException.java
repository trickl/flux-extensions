package com.trickl.exceptions;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class NoSupportingDelegateException extends Exception {

  private static final long serialVersionUID = -1761231643713163261L;

  public NoSupportingDelegateException(String message) {
    super(message);
  }

  public NoSupportingDelegateException(String message, Throwable cause) {
    super(message, cause);
  }

  public NoSupportingDelegateException(Throwable cause) {
    super(cause);
  }
}
