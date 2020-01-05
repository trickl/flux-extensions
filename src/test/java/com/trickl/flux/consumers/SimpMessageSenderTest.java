package com.trickl.flux.consumers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.publisher.TestPublisher;

@RunWith(MockitoJUnitRunner.class)
public class SimpMessageSenderTest {

  @Mock private SimpMessagingTemplate messagingTemplate;

  private static final String DESTINATION = "/topic/example";

  /** Setup the tests. */
  @Before
  public void init() {
    doNothing().when(messagingTemplate).convertAndSend(any(String.class), any(String.class));
  }

  @Test
  public void testDoesNothingWithoutSubscribers() {
    TestPublisher<String> input = TestPublisher.<String>create();

    Publisher<String> output = Flux.from(input)
        .doOnNext(new SimpMessageSender<>(messagingTemplate, DESTINATION))
        .publish()
        .refCount();

    input.next("a", "a", "a");

    verify(messagingTemplate, never()).convertAndSend(any(String.class), any(String.class));
  }

  @Test
  public void testBroadcastsOnceWithOneSubscriber() {
    TestPublisher<String> input = TestPublisher.<String>create();
    Publisher<String> output = Flux.from(input)
        .doOnNext(new SimpMessageSender<>(messagingTemplate, DESTINATION))
        .publish()
        .refCount();
    input.next("a", "a", "a");
    
    output.subscribe(new BaseSubscriber<String>() {});
    input.next("b", "b");

    verify(messagingTemplate, never()).convertAndSend(DESTINATION, "a");
    verify(messagingTemplate, times(2)).convertAndSend(DESTINATION, "b");
  }

  @Test
  public void testBroadcastsOnceWithMultipleSubscriber() {
    TestPublisher<String> input = TestPublisher.<String>create();
    Publisher<String> output = Flux.from(input)
        .doOnNext(new SimpMessageSender<>(messagingTemplate, DESTINATION))
        .publish()
        .refCount();
        
    input.next("a", "a", "a");
    
    Subscriber<String> firstSubscriber = new BaseSubscriber<String>() {};
    output.subscribe(firstSubscriber);
    
    input.next("b", "b");

    Subscriber<String> secondSubscriber = new BaseSubscriber<String>() {};
    output.subscribe(secondSubscriber);

    input.next("c", "c", "c", "c");

    secondSubscriber.onComplete();

    input.next("d", "d");

    verify(messagingTemplate, never()).convertAndSend(DESTINATION, "a");
    verify(messagingTemplate, times(2)).convertAndSend(DESTINATION, "b");
    verify(messagingTemplate, times(4)).convertAndSend(DESTINATION, "c");
    verify(messagingTemplate, times(2)).convertAndSend(DESTINATION, "d");
  }

  @Test
  public void testStopsBroadcastingWithoutSubscribers() {
    TestPublisher<String> input = TestPublisher.<String>create();
    Publisher<String> output = Flux.from(input)
        .doOnNext(new SimpMessageSender<>(messagingTemplate, DESTINATION))
        .publish()
        .refCount();
        
    input.next("a", "a", "a");
    
    BaseSubscriber<String> subscriber = new BaseSubscriber<String>() {};
    output.subscribe(subscriber);
    
    input.next("b", "b");

    subscriber.cancel();

    input.next("e", "e");
    
    input.assertCancelled();

    verify(messagingTemplate, never()).convertAndSend(DESTINATION, "a");
    verify(messagingTemplate, times(2)).convertAndSend(DESTINATION, "b");
  }

  @Test
  public void testStopsBroadcastingWithoutSubscribersScenario2() {
    TestPublisher<String> input = TestPublisher.<String>create();
    Publisher<String> output = Flux.from(input)
        .doOnNext(new SimpMessageSender<>(messagingTemplate, DESTINATION))
        .publish()
        .refCount();
        
    input.next("a", "a", "a");
    
    BaseSubscriber<String> firstSubscriber = new BaseSubscriber<String>() {};
    output.subscribe(firstSubscriber);
    
    input.next("b", "b");

    BaseSubscriber<String> secondSubscriber = new BaseSubscriber<String>() {};
    output.subscribe(secondSubscriber);

    input.next("c", "c", "c", "c");

    secondSubscriber.cancel();
    input.assertWasNotCancelled();

    input.next("d", "d");

    firstSubscriber.cancel();

    input.next("e", "e");
    input.assertCancelled();

    verify(messagingTemplate, never()).convertAndSend(DESTINATION, "a");
    verify(messagingTemplate, times(2)).convertAndSend(DESTINATION, "b");
    verify(messagingTemplate, times(4)).convertAndSend(DESTINATION, "c");
    verify(messagingTemplate, times(2)).convertAndSend(DESTINATION, "d");
    verify(messagingTemplate, never()).convertAndSend(DESTINATION, "e");
  }

  @Test
  public void testRebroadcastingOnResubscribers() {
    TestPublisher<String> input = TestPublisher.<String>create();
    Publisher<String> output = Flux.from(input)
        .doOnNext(new SimpMessageSender<>(messagingTemplate, DESTINATION))
        .publish()
        .refCount();
        
    input.next("a", "a", "a");
    
    BaseSubscriber<String> firstSubscriber = new BaseSubscriber<String>() {};
    output.subscribe(firstSubscriber);
    
    input.next("b", "b");

    firstSubscriber.cancel();
    input.assertCancelled();

    input.next("c", "c", "c", "c");

    BaseSubscriber<String> secondSubscriber = new BaseSubscriber<String>() {};
    output.subscribe(secondSubscriber);

    input.next("d", "d", "d");

    verify(messagingTemplate, never()).convertAndSend(DESTINATION, "a");
    verify(messagingTemplate, times(2)).convertAndSend(DESTINATION, "b");
    verify(messagingTemplate, never()).convertAndSend(DESTINATION, "c");
    verify(messagingTemplate, times(3)).convertAndSend(DESTINATION, "d");
  }
}
