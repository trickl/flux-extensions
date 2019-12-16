package com.trickl.flux.websocket.stomp;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;

public interface StompFrame {
  StompHeaderAccessor getHeaderAccessor();

  Message<byte[]> toMessage(ObjectMapper objectMapper) throws IOException;
}