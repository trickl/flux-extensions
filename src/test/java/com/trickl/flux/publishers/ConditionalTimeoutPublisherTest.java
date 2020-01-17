package com.trickl.flux.publishers;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

public class ConditionalTimeoutPublisherTest {
  

  @Test
  public void expectContinueWithElements() {
    TestPublisher<String> input = TestPublisher.<String>create();

    ConditionalTimeoutPublisher<String> timeoutPublisher =
        new ConditionalTimeoutPublisher<>(
            input,          
            Duration.ofSeconds(15),
            value -> true,
            TimeoutException::new,
            null,
            Schedulers.parallel());

    Publisher<String> output = Flux.from(input).mergeWith(timeoutPublisher.get());
    
    StepVerifier.withVirtualTime(() -> output)
      .then(() -> input.emit("first"))      
      .expectNoEvent(Duration.ofSeconds(10))
      .then(() -> input.emit("second"))      
      .thenAwait(Duration.ofSeconds(10))
      .then(input::complete)
      .expectComplete();
  }


  @Test
  public void expectExceptionWithoutElements() {
    TestPublisher<String> input = TestPublisher.<String>create();

    ConditionalTimeoutPublisher<String> timeoutPublisher =
        new ConditionalTimeoutPublisher<>(
            input,          
            Duration.ofSeconds(15),
            value -> true,
            TimeoutException::new,
            null,
            Schedulers.parallel());

    Publisher<String> output = Flux.from(input).mergeWith(timeoutPublisher.get());
    
    StepVerifier.withVirtualTime(() -> output)
      .then(() -> input.emit("first"))      
      .expectNoEvent(Duration.ofSeconds(10)) 
      .thenAwait(Duration.ofSeconds(10))
      .expectError(TimeoutException.class);      
  }

  @Test
  public void expectEarlyCompleteIfRequired() {
    TestPublisher<String> input = TestPublisher.<String>create();

    ConditionalTimeoutPublisher<String> timeoutPublisher =
        new ConditionalTimeoutPublisher<>(
            input,          
            Duration.ofSeconds(15),
            value -> true,
            null,
            input::complete,
            Schedulers.parallel());

    Publisher<String> output = Flux.from(input).mergeWith(timeoutPublisher.get());
    
    StepVerifier.withVirtualTime(() -> output)
      .then(() -> input.emit("something"))      
      .expectNoEvent(Duration.ofSeconds(10)) 
      .thenAwait(Duration.ofSeconds(10))
      .expectComplete();      
  }
}
