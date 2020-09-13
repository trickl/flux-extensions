package com.trickl.flux.publishers;

import com.trickl.flux.config.WebSocketConfiguration;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;

@ActiveProfiles({"unittest"})
@TestPropertySource(properties = { "spring.config.location=classpath:application.yml" })
@SpringBootTest(classes = WebSocketConfiguration.class)
public class SwitchableProcessorTest {

  @Test
  public void testNormalFlow() {
    
    UnicastProcessor<String> signalProcessor = UnicastProcessor.create();
    SwitchableProcessor<Integer> processor = SwitchableProcessor.create(signalProcessor);

    StepVerifier.create(processor)
        .then(() -> signalProcessor.sink().next("A"))
        .then(() -> processor.sink().next(1))
        .expectNext(1)       
        .then(() -> signalProcessor.sink().complete())
        .then(() -> processor.complete())
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }

  @Test
  public void testRestartAfterSinkComplete() {
    
    UnicastProcessor<String> signalProcessor = UnicastProcessor.create();
    SwitchableProcessor<Integer> processor = SwitchableProcessor.create(signalProcessor);

    StepVerifier.create(processor)
        .then(() -> signalProcessor.sink().next("A"))
        .then(() -> processor.sink().next(1))
        .then(() -> processor.sink().complete())
        .expectNext(1)
        .then(() -> signalProcessor.sink().next("B"))
        .then(() -> processor.sink().next(2))
        .then(() -> processor.sink().complete())
        .expectNext(2)
        .then(() -> signalProcessor.sink().next("C"))
        .then(() -> processor.sink().next(3))
        .then(() -> processor.sink().complete())
        .expectNext(3)
        .then(() -> signalProcessor.sink().complete())
        .then(() -> processor.complete())
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }

  @Test
  public void testLateSubscribe() {
    
    UnicastProcessor<String> signalProcessor = UnicastProcessor.create();
    SwitchableProcessor<Integer> processor = 
        SwitchableProcessor.create(signalProcessor, 1, "switchableProcessor");

    signalProcessor.sink().next("A");
    processor.sink().next(1);

    StepVerifier.create(processor)
        .expectNext(1)
        .then(() -> signalProcessor.sink().complete())
        .then(() -> processor.complete())
        .expectComplete()
        .verify(Duration.ofSeconds(30));
  }
}
