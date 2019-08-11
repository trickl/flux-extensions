package com.trickl.flux.transformers;

import org.junit.Test;
import org.reactivestreams.Publisher;

import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

public class DifferentialTransformerTest {
  DifferentialTransformer<String, String> dashSeparateString =
      new DifferentialTransformer<>((a, b) -> a + "-" + b);

  @Test
  public void correctOutputWithZeroInputs() {
    Publisher<String> input = TestPublisher.<String>create();

    Publisher<String> output = dashSeparateString.apply(input);
    
    StepVerifier.create(output)
      .expectComplete();
  }

  @Test
  public void correctOutputWithOneInputs() {
    Publisher<String> input = TestPublisher.<String>create()
        .next("a");


    Publisher<String> output = dashSeparateString.apply(input);
    
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


    Publisher<String> output = dashSeparateString.apply(input);
    
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
