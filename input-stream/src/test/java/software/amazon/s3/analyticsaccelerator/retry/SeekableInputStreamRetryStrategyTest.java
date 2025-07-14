/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.retry;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SeekableInputStreamRetryStrategyTest {

  @Test
  void testNoArgsConstructor() {
    SeekableInputStreamRetryStrategy<String> executor = new SeekableInputStreamRetryStrategy<>();
    assertNotNull(executor);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testPolicyConstructor() {
    RetryPolicy<String> policy = RetryPolicy.ofDefaults();
    SeekableInputStreamRetryStrategy<String> executor =
        new SeekableInputStreamRetryStrategy<>(policy);
    assertNotNull(executor);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testPolicyConstructorWithMultiplePolicies() {
    RetryPolicy<String> policy1 = RetryPolicy.ofDefaults();
    RetryPolicy<String> policy2 = RetryPolicy.ofDefaults();
    SeekableInputStreamRetryStrategy<String> executor =
        new SeekableInputStreamRetryStrategy<>(policy1, policy2);
    assertNotNull(executor);
  }

  @Test
  void testPolicyConstructorWithNullOuterPolicyThrowsException() {
    assertThrows(
        NullPointerException.class, () -> new SeekableInputStreamRetryStrategy<String>(null));
  }

  @Test
  void testExecuteSuccess() throws IOException {
    SeekableInputStreamRetryStrategy<String> executor = new SeekableInputStreamRetryStrategy<>();
    AtomicInteger counter = new AtomicInteger(0);

    executor.execute(counter::incrementAndGet);

    assertEquals(1, counter.get());
  }

  @Test
  void testExecuteWrapsUncheckedException() {
    SeekableInputStreamRetryStrategy<String> executor = new SeekableInputStreamRetryStrategy<>();

    IOException exception =
        assertThrows(
            IOException.class,
            () ->
                executor.execute(
                    () -> {
                      throw new RuntimeException("Test exception");
                    }));

    assertEquals("Failed to execute operation with retries", exception.getMessage());
    assertNotNull(exception.getCause());
  }

  @Test
  void testExecuteIOException() {
    SeekableInputStreamRetryStrategy<String> executor = new SeekableInputStreamRetryStrategy<>();

    IOException exception =
        assertThrows(
            IOException.class,
            () ->
                executor.execute(
                    () -> {
                      throw new IOException("Original IO exception");
                    }));

    assertEquals("Original IO exception", exception.getMessage());
  }

  @Test
  void testGetSuccess() throws IOException {
    SeekableInputStreamRetryStrategy<String> executor = new SeekableInputStreamRetryStrategy<>();
    String expected = "test result";

    String result = executor.get(() -> expected);

    assertEquals(expected, result);
  }

  @Test
  void testGetWrapsException() {
    SeekableInputStreamRetryStrategy<String> executor = new SeekableInputStreamRetryStrategy<>();

    IOException exception =
        assertThrows(
            IOException.class,
            () ->
                executor.get(
                    () -> {
                      throw new RuntimeException("Test exception");
                    }));

    assertEquals("Failed to execute operation with retries", exception.getMessage());
    assertNotNull(exception.getCause());
  }

  @Test
  void testGetIOException() {
    SeekableInputStreamRetryStrategy<String> executor = new SeekableInputStreamRetryStrategy<>();

    IOException exception =
        assertThrows(
            IOException.class,
            () ->
                executor.get(
                    () -> {
                      throw new IOException("Original IO exception");
                    }));

    assertEquals("Original IO exception", exception.getMessage());
  }

  @Test
  void testGetNullResult() throws IOException {
    SeekableInputStreamRetryStrategy<String> executor = new SeekableInputStreamRetryStrategy<>();

    String result = executor.get(() -> null);

    assertNull(result);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testGetSucceedsOnThirdAttempt() throws IOException {
    RetryPolicy<Integer> retryPolicy =
        RetryPolicy.<Integer>builder().handle(IOException.class).withMaxRetries(3).build();
    SeekableInputStreamRetryStrategy<Integer> executor =
        new SeekableInputStreamRetryStrategy<>(retryPolicy);
    AtomicInteger retryCount = new AtomicInteger(0);

    Integer result = executor.get(() -> failTwiceThenSucceed(retryCount));

    assertEquals(1, result);
    assertEquals(3, retryCount.get());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testNoRetryOnDifferentHandle() throws IOException {
    RetryPolicy<Integer> retryPolicy =
        RetryPolicy.<Integer>builder().handle(TimeoutException.class).withMaxRetries(3).build();
    SeekableInputStreamRetryStrategy<Integer> executor =
        new SeekableInputStreamRetryStrategy<>(retryPolicy);
    AtomicInteger retryCount = new AtomicInteger(0);

    assertThrows(IOException.class, () -> executor.get(() -> failTwiceThenSucceed(retryCount)));

    assertEquals(1, retryCount.get());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testOnRetryCallback() throws IOException {
    AtomicInteger retryCounter = new AtomicInteger(0);
    RetryPolicy<Integer> policy =
        RetryPolicy.<Integer>builder()
            .withMaxRetries(3)
            .onRetry(retryCounter::incrementAndGet)
            .build();
    SeekableInputStreamRetryStrategy<Integer> executor =
        new SeekableInputStreamRetryStrategy<>(policy);
    AtomicInteger attemptCounter = new AtomicInteger(0);

    Integer result = executor.get(() -> failTwiceThenSucceed(attemptCounter));

    assertEquals(1, result);
    assertEquals(3, attemptCounter.get());
    assertEquals(2, retryCounter.get());
  }

  private Integer failTwiceThenSucceed(AtomicInteger counter) throws IOException {
    int attempt = counter.incrementAndGet();
    if (attempt <= 2) {
      throw new IOException("Attempt " + attempt + " failed");
    }
    return 1;
  }
}
