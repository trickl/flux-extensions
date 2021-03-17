package com.trickl.flux.mappers;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

public class DelayCancelTest {

  @Test
  public void delaysPropagatingCancel() {
    TestPublisher<String> source = TestPublisher.create();

    Sinks.One<String> delaySink = Sinks.one();

    Publisher<String> output = DelayCancel.apply(source, delaySink.asMono().then());
    
    StepVerifier.create(output)
      .expectSubscription()
      .thenRequest(1)
      .then(() -> source.emit("A", "B", "C"))
      .expectNext("A")
      .then(() -> source.assertWasNotCancelled())
      .thenCancel()            
        .verify();

    source.assertWasNotCancelled();
    delaySink.tryEmitValue("complete");
    source.assertCancelled();
  }
}