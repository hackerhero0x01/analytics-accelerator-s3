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
package software.amazon.s3.analyticsaccelerator.util.retry;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DefaultRetryStrategyImplTest {

  @Test
  void testNoArgsConstructor() {
    DefaultRetryStrategyImpl executor = new DefaultRetryStrategyImpl();
    assertNotNull(executor);
  }

  @Test
  void testPolicyConstructor() {
    RetryPolicy policy = RetryPolicy.ofDefaults();
    DefaultRetryStrategyImpl executor = new DefaultRetryStrategyImpl(policy);
    assertNotNull(executor);

    RetryPolicy policy1 = RetryPolicy.ofDefaults();
    RetryPolicy policy2 = RetryPolicy.ofDefaults();
    executor = new DefaultRetryStrategyImpl(policy1, policy2);
    assertNotNull(executor);

    List<RetryPolicy> policyList = Arrays.asList(policy1, policy2);
    executor = new DefaultRetryStrategyImpl(policyList);
    assertNotNull(executor);

    assertThrows(NullPointerException.class, () -> new DefaultRetryStrategyImpl(null));

    ArrayList<RetryPolicy> emptyList = new ArrayList<RetryPolicy>();
    assertThrows(IllegalArgumentException.class, () -> new DefaultRetryStrategyImpl(emptyList));
  }

  @Test
  void testAmend() throws IOException {
    DefaultRetryStrategyImpl executor = new DefaultRetryStrategyImpl();
    String expected = "test result";

    AtomicInteger attempt = new AtomicInteger(0);
    assertNotNull(executor);
    assertThrows(
        IOException.class, () -> executor.get(() -> failXTimesThenSucceed(attempt, 2, expected)));

    assertEquals(1, attempt.get());

    // Reset attempt
    attempt.set(0);

    RetryPolicy policy = RetryPolicy.builder().handle(IOException.class).withMaxRetries(3).build();
    RetryStrategy newStrategy = executor.amend(policy);

    byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);

    byte[] result = newStrategy.get(() -> failXTimesThenSucceed(attempt, 2, expected));

    assertEquals(expectedBytes.length, result.length);
    assertEquals(3, attempt.get());
  }

  @Test
  void testAmendTheSameException() throws IOException {
    RetryPolicy policy = RetryPolicy.builder().handle(IOException.class).withMaxRetries(3).build();
    DefaultRetryStrategyImpl executor = new DefaultRetryStrategyImpl(policy);
    String expected = "test result";

    AtomicInteger attempt = new AtomicInteger(0);
    assertNotNull(executor);
    executor.get(() -> failXTimesThenSucceed(attempt, 2, expected));

    assertEquals(3, attempt.get());

    // Reset attempt
    attempt.set(0);

    RetryPolicy policy2 =
        RetryPolicy.builder().handle(IOException.class).withMaxRetries(10).build();
    RetryStrategy newStrategy = executor.amend(policy2);

    byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);

    byte[] result = newStrategy.get(() -> failXTimesThenSucceed(attempt, 13, expected));

    assertEquals(expectedBytes.length, result.length);
    assertEquals(14, attempt.get());
  }

  @Test
  void testAmendDifferentException() throws IOException {
    RetryPolicy policy =
        RetryPolicy.builder().handle(TimeoutException.class).withMaxRetries(3).build();
    DefaultRetryStrategyImpl executor = new DefaultRetryStrategyImpl(policy);
    String expected = "test result";

    AtomicInteger attempt = new AtomicInteger(0);
    assertNotNull(executor);
    assertThrows(
        IOException.class, () -> executor.get(() -> failXTimesThenSucceed(attempt, 2, expected)));

    assertEquals(1, attempt.get());

    // Reset attempt
    attempt.set(0);

    RetryPolicy policy2 =
        RetryPolicy.builder().handle(IOException.class).withMaxRetries(10).build();
    RetryStrategy newStrategy = executor.amend(policy2);

    byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);

    byte[] result = newStrategy.get(() -> failXTimesThenSucceed(attempt, 9, expected));

    assertEquals(expectedBytes.length, result.length);
    assertEquals(10, attempt.get());
  }

  @Test
  void testMerge() throws IOException {
    final RetryStrategy executor = new DefaultRetryStrategyImpl();
    String expected = "test result";

    AtomicInteger attempt = new AtomicInteger(0);
    assertNotNull(executor);
    assertThrows(
        IOException.class, () -> executor.get(() -> failXTimesThenSucceed(attempt, 2, expected)));

    assertEquals(1, attempt.get());

    // Reset attempt
    attempt.set(0);

    RetryPolicy policy = RetryPolicy.builder().handle(IOException.class).withMaxRetries(3).build();
    RetryStrategy retryStrategy = new DefaultRetryStrategyImpl(policy);

    RetryStrategy newStrategy = executor.merge(retryStrategy);

    byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);

    byte[] result = newStrategy.get(() -> failXTimesThenSucceed(attempt, 2, expected));

    assertEquals(expectedBytes.length, result.length);
    assertEquals(3, attempt.get());
  }

  @Test
  void testPolicyConstructorWithNullOuterPolicyThrowsException() {
    assertThrows(NullPointerException.class, () -> new DefaultRetryStrategyImpl(null));
  }

  @Test
  void testExecuteSuccess() throws IOException {
    DefaultRetryStrategyImpl executor = new DefaultRetryStrategyImpl();
    AtomicInteger counter = new AtomicInteger(0);

    executor.execute(counter::incrementAndGet);

    assertEquals(1, counter.get());
  }

  @Test
  void testExecuteWrapsUncheckedException() {
    DefaultRetryStrategyImpl executor = new DefaultRetryStrategyImpl();

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
    DefaultRetryStrategyImpl executor = new DefaultRetryStrategyImpl();

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
    DefaultRetryStrategyImpl executor = new DefaultRetryStrategyImpl();
    String expected = "test result";
    byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);

    byte[] result = executor.get(() -> expected.getBytes(StandardCharsets.UTF_8));

    assertEquals(expectedBytes.length, result.length);
  }

  @Test
  void testGetWrapsException() {
    DefaultRetryStrategyImpl executor = new DefaultRetryStrategyImpl();

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
    DefaultRetryStrategyImpl executor = new DefaultRetryStrategyImpl();

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
  void testNoRetryOnDifferentHandle() {
    String expected = "test result";
    RetryPolicy policy =
        RetryPolicy.builder().handle(TimeoutException.class).withMaxRetries(3).build();
    DefaultRetryStrategyImpl executor = new DefaultRetryStrategyImpl(policy);
    AtomicInteger retryCount = new AtomicInteger(0);
    assertThrows(
        IOException.class,
        () -> executor.get(() -> failXTimesThenSucceed(retryCount, 2, expected)));

    assertEquals(1, retryCount.get());
  }

  @Test
  void testOnRetryCallback() throws IOException {
    AtomicInteger retryCounter = new AtomicInteger(0);
    String expected = "test result";
    byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);
    RetryPolicy policy =
        RetryPolicy.builder().withMaxRetries(3).onRetry(retryCounter::incrementAndGet).build();
    DefaultRetryStrategyImpl executor = new DefaultRetryStrategyImpl(policy);
    AtomicInteger attemptCounter = new AtomicInteger(0);

    byte[] result = executor.get(() -> failXTimesThenSucceed(attemptCounter, 2, expected));

    assertEquals(expectedBytes.length, result.length);
    assertEquals(3, attemptCounter.get());
    assertEquals(2, retryCounter.get());
  }

  private byte[] failXTimesThenSucceed(AtomicInteger counter, int failCount, String toByteArray)
      throws IOException {
    int attempt = counter.incrementAndGet();
    if (attempt <= failCount) {
      throw new IOException("Attempt " + attempt + " failed");
    }
    return toByteArray.getBytes(StandardCharsets.UTF_8);
  }
}
