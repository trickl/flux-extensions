package com.trickl.flux.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WebSocketConfiguration {

  @Bean
  ObjectMapper objectMapper() {
    return new ObjectMapper();
  }
}
