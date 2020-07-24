package com.trickl.flux.websocket.sockjs;

public class SockJsConversionException extends RuntimeException {

  private static final long serialVersionUID = -1761231643713163261L;

  public SockJsConversionException(String message) {
    super(message);
  }

  public SockJsConversionException(String message, Throwable cause) {
    super(message, cause);
  }

  public SockJsConversionException(Throwable cause) {
    super(cause);
  }
}
