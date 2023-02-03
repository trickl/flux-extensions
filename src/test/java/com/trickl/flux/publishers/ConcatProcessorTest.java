package com.trickl.flux.publishers;

import com.trickl.flux.config.WebSocketConfiguration;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import reactor.test.StepVerifier;

@ActiveProfiles({"unittest"})
@TestPropertySource(properties = { "spring.config.location=classpath:application.yml" })
@SpringBootTest(classes = WebSocketConfiguration.class)
public class ConcatProcessorTest {

  @Test
  public void testRestartAfterSinkComplete() {
    
    ConcatProcessor<Integer> processor = ConcatProcessor.create();

    StepVerifier.create(processor)
        .then(() -> processor.sink().tryEmitNext(1))
        .then(() -> processor.sink().tryEmitComplete())
        .expectNext(1)
        .then(() -> processor.sink().tryEmitNext(2))
        .then(() -> processor.sink().tryEmitComplete())
        .expectNext(2)
        .then(() -> processor.sink().tryEmitNext(3))
        .then(() -> processor.sink().tryEmitComplete())
        .expectNext(3)
        .then(() -> processor.complete())
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }

  @Test
  public void testLateSubscribe() {
    
    ConcatProcessor<Integer> processor = 
        ConcatProcessor.create(1);

    processor.sink().tryEmitNext(1);

    StepVerifier.create(processor)
        .expectNext(1)
        .then(() -> processor.complete())
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }
}
