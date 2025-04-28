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
package software.amazon.s3.analyticsaccelerator.io.physical.data;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;

public class BlobStoreIndexCacheTest {

  private PhysicalIOConfiguration mockConfig;
  private BlobStoreIndexCache cache;
  private static final long TIMEOUT_MS = 1000;
  private static final long CAPACITY_BYTES = 1024 * 1024; // 1MB

  @BeforeEach
  void setUp() {
    mockConfig = mock(PhysicalIOConfiguration.class);
    when(mockConfig.getCacheDataTimeoutMilliseconds()).thenReturn(TIMEOUT_MS);
    when(mockConfig.getMemoryCapacityBytes()).thenReturn(CAPACITY_BYTES);
    cache = new BlobStoreIndexCache(mockConfig);
  }

  @Test
  @DisplayName("Test basic put and contains operations")
  void testBasicOperations() {
    BlockKey key = new BlockKey(mock(ObjectKey.class), mock(Range.class));
    cache.put(key, 100);
    assertTrue(cache.contains(key));
  }

  @Test
  @DisplayName("Test cache expiration")
  void testCacheExpiration() throws InterruptedException {
    BlockKey key = new BlockKey(mock(ObjectKey.class), mock(Range.class));
    cache.put(key, 100);
    Thread.sleep(TIMEOUT_MS + 100);
    assertFalse(cache.contains(key));
  }

  @Test
  @DisplayName("Test maximum weight limit")
  void testMaximumWeight() {
    int entrySize = 1024; // 1KB per entry
    int numEntries = (int) (CAPACITY_BYTES / entrySize + 2); // Exceed capacity by 2 entries

    for (int i = 0; i < numEntries; i++) {
      cache.put(new BlockKey(mock(ObjectKey.class), mock(Range.class)), entrySize);
    }

    assertTrue(cache.getCurrentWeight() <= cache.getMaximumWeight());
  }

  @Test
  @DisplayName("Test concurrent put and get operations")
  void testConcurrentPutAndGet() throws InterruptedException {
    int numThreads = 10;
    int operationsPerThread = 10000;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(numThreads * 2);
    ConcurrentHashMap<BlockKey, Integer> expectedValues = new ConcurrentHashMap<>();
    List<Future<?>> futures = new ArrayList<>();

    ExecutorService executor = Executors.newFixedThreadPool(numThreads * 2);

    try {
      // Writer threads
      for (int i = 0; i < numThreads; i++) {
        Future<?> future =
            executor.submit(
                () -> {
                  try {
                    startLatch.await();
                    Random random = new Random();
                    for (int j = 0; j < operationsPerThread; j++) {
                      BlockKey key = new BlockKey(mock(ObjectKey.class), mock(Range.class));
                      int value = random.nextInt(1000);
                      cache.put(key, value);
                      expectedValues.put(key, value);
                      Thread.yield();
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    completionLatch.countDown();
                  }
                });
        futures.add(future);
      }

      // Reader threads
      for (int i = 0; i < numThreads; i++) {
        Future<?> future =
            executor.submit(
                () -> {
                  try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                      BlockKey key = new BlockKey(mock(ObjectKey.class), mock(Range.class));
                      if (cache.contains(key)) {
                        cache.getIfPresent(key);
                      }
                      Thread.yield();
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    completionLatch.countDown();
                  }
                });
        futures.add(future);
      }

      startLatch.countDown();
      assertTrue(completionLatch.await(30, TimeUnit.SECONDS));

      // Check for any exceptions in futures
      for (Future<?> future : futures) {
        assertDoesNotThrow(() -> future.get(1, TimeUnit.SECONDS));
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  @DisplayName("Test weight-based eviction under heavy load")
  void testWeightBasedEviction() throws InterruptedException {
    int entrySize = 1024; // 1KB per entry
    int numEntries = (int) (CAPACITY_BYTES / entrySize * 2); // Double the capacity
    Set<BlockKey> presentKeys = ConcurrentHashMap.newKeySet();

    // Fill cache beyond capacity
    for (int i = 0; i < numEntries; i++) {
      BlockKey key = new BlockKey(mock(ObjectKey.class), mock(Range.class));
      cache.put(key, entrySize);
      presentKeys.add(key);
    }

    assertTrue(cache.getCurrentWeight() <= cache.getMaximumWeight());

    // Verify some entries were evicted
    int presentCount = 0;
    for (BlockKey key : presentKeys) {
      if (cache.contains(key)) {
        presentCount++;
      }
    }
    assertTrue(presentCount < numEntries);
  }

  @Test
  @DisplayName("Test concurrent eviction and access")
  void testConcurrentEvictionAndAccess() throws InterruptedException {
    int numThreads = 5;
    int entriesPerThread = 1000;
    int entrySize = (int) (CAPACITY_BYTES / (numThreads * entriesPerThread / 2));
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(numThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<?>> futures = new ArrayList<>();

    try {
      // Submit all tasks first
      for (int i = 0; i < numThreads; i++) {
        Future<?> future =
            executor.submit(
                () -> {
                  try {
                    startLatch.await();
                    Random random = new Random();
                    for (int j = 0; j < entriesPerThread; j++) {
                      BlockKey key = new BlockKey(mock(ObjectKey.class), mock(Range.class));
                      cache.put(key, entrySize);

                      BlockKey randomKey = new BlockKey(mock(ObjectKey.class), mock(Range.class));
                      cache.contains(randomKey);
                      long currentWeight = cache.getCurrentWeight();

                      assertTrue(currentWeight <= cache.getMaximumWeight());
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    completionLatch.countDown();
                  }
                });
        futures.add(future);
      }

      // Start all threads
      startLatch.countDown();

      // Wait for completion
      assertTrue(completionLatch.await(30, TimeUnit.SECONDS));

      // Check futures after all tasks are complete
      for (Future<?> future : futures) {
        assertDoesNotThrow(() -> future.get(5, TimeUnit.SECONDS));
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  @DisplayName("Test edge case: rapid put/remove cycles")
  void testRapidPutRemoveCycles()
      throws InterruptedException, ExecutionException, TimeoutException {
    int numThreads = 4;
    int cyclesPerThread = 1000;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch completionLatch = new CountDownLatch(numThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    List<Future<?>> futures = new ArrayList<>();

    // Track the last successful put operation for each thread
    ConcurrentHashMap<Integer, BlockKey> lastSuccessfulPuts = new ConcurrentHashMap<>();

    try {
      // Submit all tasks first
      for (int i = 0; i < numThreads; i++) {
        final int threadId = i;
        Future<?> future =
            executor.submit(
                () -> {
                  try {
                    startLatch.await();
                    for (int j = 0; j < cyclesPerThread; j++) {
                      BlockKey key = new BlockKey(mock(ObjectKey.class), mock(Range.class));
                      cache.put(key, 100);

                      // If we can verify the put was successful, update the last successful put
                      if (cache.contains(key)) {
                        lastSuccessfulPuts.put(threadId, key);
                      }

                      // Less frequent cleanup to reduce interference
                      if (j % 200 == 0) {
                        Thread.yield(); // Give other threads a chance
                      }
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    completionLatch.countDown();
                  }
                });
        futures.add(future);
      }

      // Start all threads
      startLatch.countDown();

      // Wait for completion
      assertTrue(completionLatch.await(30, TimeUnit.SECONDS));

      // Verify that at least one thread had a successful put operation
      assertFalse(
          lastSuccessfulPuts.isEmpty(),
          "At least one thread should have had a successful put operation");

      // Check the most recent successful puts
      int successCount = 0;
      for (BlockKey key : lastSuccessfulPuts.values()) {
        if (cache.contains(key)) {
          successCount++;
        }
      }

      // Assert that at least one recent put is still in the cache
      assertTrue(
          successCount > 0,
          "At least one recent put operation should still be present in the cache");

      // Check futures for any exceptions
      for (Future<?> future : futures) {
        future.get(5, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  @DisplayName("Test weight calculation accuracy")
  void testWeightCalculation() {
    int entrySize = 100;
    int numEntries = 10;
    long initialWeight = cache.getCurrentWeight();

    for (int i = 0; i < numEntries; i++) {
      cache.put(new BlockKey(mock(ObjectKey.class), mock(Range.class)), entrySize);
    }

    long expectedWeight = (long) numEntries * entrySize;
    long actualWeight = cache.getCurrentWeight();

    assertTrue(actualWeight >= initialWeight);
    assertTrue(actualWeight <= expectedWeight);
  }

  @Test
  @DisplayName("Test edge case: zero weight entries")
  void testZeroWeightEntries() {
    BlockKey key = new BlockKey(mock(ObjectKey.class), mock(Range.class));
    cache.put(key, 0);
    assertTrue(cache.contains(key));
    assertEquals(0, cache.getCurrentWeight());
  }

  @Test
  @DisplayName("Test edge case: negative weight entries")
  void testNegativeWeightEntries() {
    BlockKey key = new BlockKey(mock(ObjectKey.class), mock(Range.class));
    assertThrows(IllegalArgumentException.class, () -> cache.put(key, -1));
  }

  @Test
  @DisplayName("Test edge case: maximum possible weight")
  void testMaximumPossibleWeight() {
    BlockKey key = new BlockKey(mock(ObjectKey.class), mock(Range.class));
    cache.put(key, Integer.MAX_VALUE);
    assertTrue(cache.getCurrentWeight() <= cache.getMaximumWeight());
  }

  @Test
  @DisplayName("Test null key handling")
  void testNullKeyHandling() {
    assertThrows(NullPointerException.class, () -> cache.put(null, 100));
    assertThrows(NullPointerException.class, () -> cache.contains(null));
    assertThrows(NullPointerException.class, () -> cache.getIfPresent(null));
  }

  @AfterEach
  void tearDown() {
    if (cache != null) {
      cache.cleanUp();
    }
  }

  @Test
  @DisplayName("Test getMaximumWeight and getCurrentWeight methods")
  void testWeightMethods() throws InterruptedException {
    // Test getMaximumWeight
    long maxWeight = cache.getMaximumWeight();
    assertEquals(CAPACITY_BYTES, maxWeight, "Maximum weight should match configured capacity");

    // Test getCurrentWeight with empty cache
    long initialWeight = cache.getCurrentWeight();
    assertEquals(0, initialWeight, "Initial weight should be 0");

    // Add some entries and test getCurrentWeight
    BlockKey key = new BlockKey(mock(ObjectKey.class), mock(Range.class));
    int entryWeight = 100;
    cache.put(key, entryWeight);

    Thread.sleep(10);
    long weightAfterPut = cache.getCurrentWeight();
    assertEquals(entryWeight, weightAfterPut, "Weight should reflect added entry");
  }
}
