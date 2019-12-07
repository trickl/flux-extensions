package com.trickl.flux.publishers;

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
import reactor.test.publisher.TestPublisher;

@RunWith(MockitoJUnitRunner.class)
public class SimpMessagingPublisherTest {

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

    new SimpMessagingPublisher<>(input, messagingTemplate, DESTINATION).get();

    input.next("a", "a", "a");

    verify(messagingTemplate, never()).convertAndSend(any(String.class), any(String.class));
  }

  @Test
  public void testBroadcastsOnceWithOneSubscriber() {
    TestPublisher<String> input = TestPublisher.<String>create();
    Publisher<String> broadcaster =
        new SimpMessagingPublisher<>(input, messagingTemplate, DESTINATION).get();
    input.next("a", "a", "a");
    
    broadcaster.subscribe(new BaseSubscriber<String>() {});
    input.next("b", "b");

    verify(messagingTemplate, never()).convertAndSend(DESTINATION, "a");
    verify(messagingTemplate, times(2)).convertAndSend(DESTINATION, "b");
  }

  @Test
  public void testBroadcastsOnceWithMultipleSubscriber() {
    TestPublisher<String> input = TestPublisher.<String>create();
    Publisher<String> broadcaster =
        new SimpMessagingPublisher<>(input, messagingTemplate, DESTINATION).get();
        
    input.next("a", "a", "a");
    
    Subscriber<String> firstSubscriber = new BaseSubscriber<String>() {};
    broadcaster.subscribe(firstSubscriber);
    
    input.next("b", "b");

    Subscriber<String> secondSubscriber = new BaseSubscriber<String>() {};
    broadcaster.subscribe(secondSubscriber);

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
    Publisher<String> broadcaster =
        new SimpMessagingPublisher<>(input, messagingTemplate, DESTINATION).get();
        
    input.next("a", "a", "a");
    
    BaseSubscriber<String> subscriber = new BaseSubscriber<String>() {};
    broadcaster.subscribe(subscriber);
    
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
    Publisher<String> broadcaster =
        new SimpMessagingPublisher<>(input, messagingTemplate, DESTINATION).get();
        
    input.next("a", "a", "a");
    
    BaseSubscriber<String> firstSubscriber = new BaseSubscriber<String>() {};
    broadcaster.subscribe(firstSubscriber);
    
    input.next("b", "b");

    BaseSubscriber<String> secondSubscriber = new BaseSubscriber<String>() {};
    broadcaster.subscribe(secondSubscriber);

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
    Publisher<String> broadcaster =
        new SimpMessagingPublisher<>(input, messagingTemplate, DESTINATION).get();
        
    input.next("a", "a", "a");
    
    BaseSubscriber<String> firstSubscriber = new BaseSubscriber<String>() {};
    broadcaster.subscribe(firstSubscriber);
    
    input.next("b", "b");

    firstSubscriber.cancel();
    input.assertCancelled();

    input.next("c", "c", "c", "c");

    BaseSubscriber<String> secondSubscriber = new BaseSubscriber<String>() {};
    broadcaster.subscribe(secondSubscriber);

    input.next("d", "d", "d");

    verify(messagingTemplate, never()).convertAndSend(DESTINATION, "a");
    verify(messagingTemplate, times(2)).convertAndSend(DESTINATION, "b");
    verify(messagingTemplate, never()).convertAndSend(DESTINATION, "c");
    verify(messagingTemplate, times(3)).convertAndSend(DESTINATION, "d");
  }
}
