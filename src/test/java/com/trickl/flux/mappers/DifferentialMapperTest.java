package com.trickl.flux.mappers;

import org.junit.Test;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

public class DifferentialMapperTest {
  DifferentialMapper<String, String> dashSeparateString =
      new DifferentialMapper<>((a, b) -> Mono.just(a + "-" + b), "null");

  @Test
  public void correctOutputWithZeroInputs() {
    Publisher<String> input = TestPublisher.<String>create();

    Publisher<String> output = Flux.from(input).flatMap(dashSeparateString);
    
    StepVerifier.create(output)
      .expectComplete();
  }

  @Test
  public void correctOutputWithOneInputs() {
    Publisher<String> input = TestPublisher.<String>create()
        .next("a");


    Publisher<String> output = Flux.from(input).flatMap(dashSeparateString);
    
    StepVerifier.create(output)
      .expectNext("null-a")
      .expectNext("a-null")
      .expectComplete();
  }

  @Test
  public void correctOutputWithMultipleInputs() {
    Publisher<String> input = TestPublisher.<String>create()
        .next("a")
        .next("b")
        .next("c")
        .next("d")
        .next("e");


    Publisher<String> output = Flux.from(input).flatMap(dashSeparateString);
    
    StepVerifier.create(output)
      .expectNext("null-a")
      .expectNext("a-b")
      .expectNext("b-c")
      .expectNext("c-d")
      .expectNext("d-e")
      .expectNext("e-null")
      .expectComplete();
  }
}
