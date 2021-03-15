package com.trickl.flux.publishers;

import com.trickl.flux.config.WebSocketConfiguration;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import reactor.core.publisher.DirectProcessor;
import reactor.test.StepVerifier;

@ActiveProfiles({"unittest"})
@TestPropertySource(properties = { "spring.config.location=classpath:application.yml" })
@SpringBootTest(classes = WebSocketConfiguration.class)
public class MergeEagerCompleteTest {

  @Test
  void testMergesAndCompletesEagerlyLeft() {

    DirectProcessor<Integer> first = DirectProcessor.create();
    DirectProcessor<Integer> second = DirectProcessor.create();
    MergeEagerComplete<Integer> mergeEagerComplete = new MergeEagerComplete<>(first, second);
    

    StepVerifier.create(mergeEagerComplete)
        .then(() -> first.sink().next(1))
        .expectNext(1)
        .then(() -> first.sink().next(2))
        .expectNext(2)
        .then(() -> second.sink().next(3))
        .expectNext(3)
        .then(() -> second.sink().next(4))
        .expectNext(4)
        .then(() -> second.sink().complete())
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }


  @Test
  void testMergesAndCompletesEagerlyRight() {

    DirectProcessor<Integer> first = DirectProcessor.create();
    DirectProcessor<Integer> second = DirectProcessor.create();
    MergeEagerComplete<Integer> mergeEagerComplete = new MergeEagerComplete<>(first, second);
    

    StepVerifier.create(mergeEagerComplete)
        .then(() -> first.sink().next(1))
        .expectNext(1)
        .then(() -> first.sink().next(2))
        .expectNext(2)
        .then(() -> second.sink().next(3))
        .expectNext(3)
        .then(() -> second.sink().next(4))
        .expectNext(4)
        .then(() -> first.sink().complete())
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }

  @Test
  void testHandlesError() {

    DirectProcessor<Integer> first = DirectProcessor.create();
    DirectProcessor<Integer> second = DirectProcessor.create();
    MergeEagerComplete<Integer> mergeEagerComplete = new MergeEagerComplete<>(first, second);
    

    StepVerifier.create(mergeEagerComplete)
        .then(() -> first.sink().next(1))
        .expectNext(1)
        .then(() -> first.sink().next(2))
        .expectNext(2)
        .then(() -> second.sink().next(3))
        .expectNext(3)
        .then(() -> second.sink().next(4))
        .expectNext(4)
        .then(() -> first.sink().error(new RuntimeException("First Failure")))
        .expectError(RuntimeException.class)
        .verify(Duration.ofSeconds(30));
  }
}
