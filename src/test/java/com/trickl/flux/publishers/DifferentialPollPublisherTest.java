package com.trickl.flux.publishers;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

@RunWith(MockitoJUnitRunner.class)
public class DifferentialPollPublisherTest {

  private static final Instant START_TIME = Instant.parse("2019-01-01T00:00:00Z");

  private class TestPoller {
    public Publisher<String> getDaysSinceLast(Instant from, Instant to) {      
      if (from != null && to != null) {
        long days = ChronoUnit.DAYS.between(from, to);
        return Mono.<String>just(MessageFormat.format("{0} days", new Object[] {days}));
      } else if (from != null) {
        return Mono.<String>just(MessageFormat.format("from {0}", from));
      } else if (to != null) {
        return Mono.<String>just(MessageFormat.format("to {0}", to));
      }
      return Flux.<String>empty();
    }
  }

  private TestPoller poller;

  /**
   * Setup the tests.
   */
  @Before
  public void init() {
    poller = new TestPoller();
  }

  @Test
  public void testSubscribesCorrectly() {
    TestPublisher<Instant> instantPublisher = TestPublisher.<Instant>create();    

    DifferentialPollPublisher<String> publisher =
        new DifferentialPollPublisher<>(
          instantPublisher, poller::getDaysSinceLast);
    Flux<String> output = publisher.get();

    StepVerifier.create(output)
      .then(instantPublisher::complete)
      .expectComplete()
      .verify();
  }

  @Test
  public void testGeneratesFirstCorrectly() {
    TestPublisher<Instant> instantPublisher = TestPublisher.<Instant>create();    

    DifferentialPollPublisher<String> publisher =
        new DifferentialPollPublisher<>(
          instantPublisher, poller::getDaysSinceLast);
    Flux<String> output = publisher.get();

    StepVerifier.create(output)      
      .then(() -> instantPublisher.next(START_TIME))
      .expectNext("to 2019-01-01T00:00:00Z")
      .then(instantPublisher::complete)
      .expectNext("from 2019-01-01T00:00:00Z")
      .expectComplete()
      .verify();   
  }

  @Test
  public void testGeneratesMultipleCorrectly() {
    TestPublisher<Instant> instantPublisher = TestPublisher.<Instant>create();    

    DifferentialPollPublisher<String> publisher =
        new DifferentialPollPublisher<>(
          instantPublisher, poller::getDaysSinceLast);
    Flux<String> output = publisher.get();

    StepVerifier.create(output)
      .then(() -> instantPublisher.next(START_TIME))
      .expectNext("to 2019-01-01T00:00:00Z")
      .then(() -> instantPublisher.next(START_TIME.plus(Duration.ofDays(3))))
      .expectNext("3 days")
      .then(() -> instantPublisher.next(START_TIME.plus(Duration.ofDays(4))))
      .expectNext("1 days")
      .then(() -> instantPublisher.next(START_TIME.plus(Duration.ofDays(5))))
      .expectNext("1 days")
      .then(() -> instantPublisher.next(START_TIME.plus(Duration.ofDays(8))))
      .expectNext("3 days")
      .then(instantPublisher::complete)
      .expectNext("from 2019-01-09T00:00:00Z")      
      .expectComplete()
      .verify();   
  }
}
