package com.trickl.flux.websocket.sockjs;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import org.apache.el.parser.ParseException;
import org.reactivestreams.Publisher;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.socket.sockjs.SockJsException;
import org.springframework.web.socket.sockjs.frame.Jackson2SockJsMessageCodec;
import org.springframework.web.socket.sockjs.frame.SockJsFrame;
import org.springframework.web.socket.sockjs.frame.SockJsMessageCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Log
@RequiredArgsConstructor
public class SockJsInputTransformer implements Function<Publisher<String>, Flux<String>> {

  private final ObjectMapper objectMapper;

  private final Supplier<String> openMessageSupplier;

  private final Supplier<String> hearbeatMessageSupplier;

  private final Function<CloseStatus, String> closeMessageFunction;

  @Override
  public Flux<String> apply(Publisher<String> source) {
    SockJsMessageCodec codec = new Jackson2SockJsMessageCodec(objectMapper);
    return Flux.from(source).flatMap(payload -> handleFrame(payload, codec));
  }

  protected Publisher<String> handleFrame(String payload, SockJsMessageCodec codec) {
    SockJsFrame frame = new SockJsFrame(payload);
    switch (frame.getType()) {
      case OPEN:
        return handleOpenFrame();
      case HEARTBEAT:
        return handleHeartbeatFrame();
      case MESSAGE:
        return handleMessageFrame(frame, codec);
      case CLOSE:
        return handleCloseFrame(frame, codec);
      default:
        break;
    }
    return Flux.error(new SockJsException("Bad SockJS Frame", new ParseException(payload)));
  }

  private Publisher<String> handleOpenFrame() {
    if (log.isLoggable(Level.FINE)) {
      log.fine("Processing SockJS open frame.");
    }
    return Mono.justOrEmpty(openMessageSupplier != null ? openMessageSupplier.get() : null);
  }

  private Publisher<String> handleHeartbeatFrame() {
    if (log.isLoggable(Level.FINE)) {
      log.fine("Processing SockJS heartbeat frame.");
    }
    return Mono.justOrEmpty(hearbeatMessageSupplier != null ? hearbeatMessageSupplier.get() : null);
  }

  private Publisher<String> handleMessageFrame(SockJsFrame frame, SockJsMessageCodec codec) {
    String[] messages = null;
    String frameData = frame.getFrameData();
    if (frameData != null) {
      try {
        messages = codec.decode(frameData);
      } catch (IOException ex) {
        if (log.isLoggable(Level.WARNING)) {
          log.log(
              Level.WARNING, "Failed to decode data for SockJS \"message\" frame: " + frame, ex);
        }
      }
    }

    if (messages == null) {
      return Mono.empty();
    }

    return Flux.fromArray(messages);
  }

  private Publisher<String> handleCloseFrame(SockJsFrame frame, SockJsMessageCodec codec) {
    CloseStatus closeStatus = CloseStatus.NO_STATUS_CODE;
    try {
      String frameData = frame.getFrameData();
      if (frameData != null) {
        String[] data = codec.decode(frameData);
        if (data != null && data.length == 2) {
          closeStatus = new CloseStatus(Integer.parseInt(data[0]), data[1]);
        }
        if (log.isLoggable(Level.FINE)) {
          log.fine("Processing SockJS close frame with " + closeStatus);
        }

        if (closeStatus != CloseStatus.NORMAL) {
          String errorMessage = MessageFormat.format(
              "Unexpected SockJS Close ({0}) - {1}", 
              closeStatus.getCode(), closeStatus.getReason());
          log.warning(errorMessage);
          return Mono.error(new Exception(errorMessage));
        }
      }
    } catch (IOException ex) {
      if (log.isLoggable(Level.WARNING)) {
        log.log(Level.WARNING, "Failed to decode data for " + frame, ex);
      }
    }
    return Mono.justOrEmpty(
        closeMessageFunction != null ? closeMessageFunction.apply(closeStatus) : null);
  }
}
