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
package software.amazon.s3.analyticsaccelerator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_GB;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.data.Blob;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.io.physical.data.MemoryManager;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class BlobStoreEvictionTest {

  @Mock private ObjectClient mockObjectClient;
  @Mock private Telemetry mockTelemetry;
  @Mock private PhysicalIOConfiguration mockConfiguration;
  @Mock private MemoryManager mockMemoryManager;

  private BlobStore blobStore;

  /** Sets up prerequisites to run the tests */
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(mockConfiguration.getMaxMemoryLimitAAL()).thenReturn((long) 9 * ONE_MB);
    BlobStore realBlobStore = new BlobStore(mockObjectClient, mockTelemetry, mockConfiguration);
    blobStore = spy(realBlobStore);
    doReturn(1024 * 1024L).when(blobStore).getBlobSize(any());
  }

  @Test
  void testBlobStoreMemoryLimitMaxMemoryLimitWhenConfigured() {
    // Given
    long configuredMaxMemory = 1000000L; // 1 MB
    PhysicalIOConfiguration config = mock(PhysicalIOConfiguration.class);
    when(config.getMaxMemoryLimitAAL()).thenReturn(configuredMaxMemory);

    // When
    BlobStore blobStore = new BlobStore(mock(ObjectClient.class), TestTelemetry.DEFAULT, config);

    // Then
    long expectedReserveMemory =
        Math.min((long) Math.ceil(0.10 * configuredMaxMemory), BlobStore.RESERVE_MEMORY);
    long expectedMaxMemoryLimit = configuredMaxMemory - expectedReserveMemory;
    assertEquals(expectedMaxMemoryLimit, blobStore.getBlobStoreMaxMemoryLimit());
  }

  @Test
  void testBlobStoreMemoryLimitMaxMemoryLimitWhenNotConfigured() {
    // Given
    PhysicalIOConfiguration config = mock(PhysicalIOConfiguration.class);
    when(config.getMaxMemoryLimitAAL()).thenReturn(Long.MAX_VALUE);

    // When
    BlobStore blobStore = new BlobStore(mock(ObjectClient.class), TestTelemetry.DEFAULT, config);

    /*MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    long maxHeapMemory = memoryBean.getHeapMemoryUsage().getMax();
    long expectedLimit = (long) Math.ceil(0.10 * maxHeapMemory);*/

    // Then
    assertEquals(3 * ONE_GB, blobStore.getBlobStoreMaxMemoryLimit());
  }

  @Test
  public void testEvictionTriggered() {
    setUp();

    int blobCount = 10;
    for (int i = 0; i < blobCount; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);
      Blob blob = blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blob.updateActiveReaders(-1);
      blobStore.updateMemoryUsage(1024 * 1024L);
    }

    // Verify that eviction was triggered
    assertTrue(blobStore.blobCount() < blobCount);
    assertTrue(blobStore.getMemoryUsage() < blobStore.getBlobStoreMaxMemoryLimit());
  }

  @Test
  public void testEvictionPreservesActiveReaders() throws InterruptedException {
    setUp();

    for (int i = 0; i < 5; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);

      blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blobStore.updateMemoryUsage(1024 * 1024L);
    }

    // Add more blobs to trigger eviction
    for (int i = 5; i < 10; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);

      Blob blob = blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blob.updateActiveReaders(-1);
      blobStore.updateMemoryUsage(1024 * 1024L);
    }
    Thread.sleep(1);

    for (int i = 0; i < 5; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      assertNotNull(blobStore.getBlobMap().get(objectKey));
    }
  }

  @Test
  public void testEvictionOrder() throws InterruptedException {
    setUp();
    // Add blobs in a specific order
    for (int i = 0; i < 5; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);

      Blob blob = blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blob.updateActiveReaders(-1);
      blobStore.updateMemoryUsage(1024 * 1024L);
    }

    // Access blobs in reverse order
    for (int i = 4; i >= 0; i--) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);

      Blob blob = blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blob.updateActiveReaders(-1);
    }

    // Add more blobs to trigger eviction
    for (int i = 5; i < 8; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);

      blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blobStore.updateMemoryUsage(1024 * 1024L);
    }

    Thread.sleep(1);

    // Verify that the least recently used blobs were evicted
    assertNotNull(blobStore.getBlobMap().get(new ObjectKey(S3URI.of("bucket", "key0"), "etag0")));
    assertNotNull(blobStore.getBlobMap().get(new ObjectKey(S3URI.of("bucket", "key1"), "etag1")));
    assertNull(blobStore.getBlobMap().get(new ObjectKey(S3URI.of("bucket", "key2"), "etag2")));
    assertNull(blobStore.getBlobMap().get(new ObjectKey(S3URI.of("bucket", "key3"), "etag3")));
    assertNull(blobStore.getBlobMap().get(new ObjectKey(S3URI.of("bucket", "key4"), "etag4")));
  }

  @Test
  public void testAccessOrderingWithoutEviction() {
    setUp();
    // Set a larger memory limit to avoid eviction
    when(mockConfiguration.getMaxMemoryLimitAAL()).thenReturn((long) 20 * ONE_MB);

    // Add blobs in a specific order
    for (int i = 0; i < 5; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      // when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);

      blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blobStore.updateMemoryUsage(1024 * 1024L);
    }

    // Access blobs in reverse order
    for (int i = 4; i >= 0; i--) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);

      blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
    }

    // Verify the order by checking the internal order of the map
    List<ObjectKey> expectedOrder = new ArrayList<>();
    for (int i = 4; i >= 0; i--) {
      expectedOrder.add(new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i));
    }

    List<ObjectKey> actualOrder = blobStore.getKeyOrder();

    assertEquals(
        expectedOrder,
        actualOrder,
        "The access order of the blobs should match the expected order");
  }

  @Test
  public void testAsyncEvictionTriggered() throws InterruptedException {
    setUp();
    CountDownLatch evictionLatch = new CountDownLatch(1);
    AtomicBoolean evictionCalled = new AtomicBoolean(false);

    doAnswer(
            invocation -> {
              evictionCalled.set(true);
              evictionLatch.countDown();
              return null;
            })
        .when(blobStore)
        .evictBlobs();

    int blobCount = 10;
    for (int i = 0; i < blobCount; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      // when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);
      Blob blob = blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blob.updateActiveReaders(-1);
      blobStore.updateMemoryUsage(1024 * 1024L);
    }

    assertTrue(evictionLatch.await(5, TimeUnit.SECONDS), "Eviction should have been triggered");
    assertTrue(evictionCalled.get(), "Eviction method should have been called");
  }

  @Test
  public void testMultipleEvictionRequests() throws InterruptedException {
    setUp();
    CountDownLatch evictionLatch = new CountDownLatch(1);
    AtomicBoolean evictionInProgress = new AtomicBoolean(false);
    AtomicInteger evictionCount = new AtomicInteger(0);

    doAnswer(
            invocation -> {
              if (evictionInProgress.compareAndSet(false, true)) {
                evictionCount.incrementAndGet();
                Thread.sleep(100); // Simulate some work
                evictionInProgress.set(false);
                evictionLatch.countDown();
              }
              return null;
            })
        .when(blobStore)
        .evictBlobs();

    // Trigger multiple eviction requests rapidly
    for (int i = 0; i < 5; i++) {
      blobStore.updateMemoryUsage(2 * 1024 * 1024L); // 2MB, should trigger eviction
    }

    assertTrue(
        evictionLatch.await(5, TimeUnit.SECONDS),
        "At least one eviction should have been triggered");
    assertEquals(1, evictionCount.get(), "Only one eviction should have been executed");
  }

  @Test
  public void testEvictionExecutorShutdown() throws InterruptedException {
    setUp();
    CountDownLatch evictionLatch = new CountDownLatch(1);

    doAnswer(
            invocation -> {
              Thread.sleep(100); // Simulate some work
              evictionLatch.countDown();
              return null;
            })
        .when(blobStore)
        .evictBlobs();

    // Trigger eviction
    blobStore.updateMemoryUsage(8 * 1024 * 1024L); // 8MB, should trigger eviction

    // Close the BlobStore while eviction is in progress
    blobStore.close();

    // Verify that the eviction task was interrupted
    assertFalse(
        evictionLatch.await(2, TimeUnit.SECONDS), "Eviction task should have been interrupted");
  }

  @Test
  public void testEvictionExceptionHandling() throws InterruptedException {
    setUp();
    CountDownLatch exceptionLatch = new CountDownLatch(1);

    doAnswer(
            invocation -> {
              throw new RuntimeException("Simulated eviction error");
            })
        .when(blobStore)
        .evictBlobs();

    // Override the executor to capture the submitted task
    ExecutorService mockExecutor = mock(ExecutorService.class);
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              new Thread(
                      () -> {
                        try {
                          task.run();
                        } finally {
                          exceptionLatch.countDown();
                        }
                      })
                  .start();
              return null;
            })
        .when(mockExecutor)
        .execute(any(Runnable.class));

    // Inject the mock executor
    try {
      Field executorField = BlobStore.class.getDeclaredField("evictionExecutor");
      executorField.setAccessible(true);
      executorField.set(blobStore, mockExecutor);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      fail("Failed to inject mock executor");
    }

    // Trigger eviction
    blobStore.updateMemoryUsage(8 * 1024 * 1024L); // 8MB, should trigger eviction

    // Wait for the exception to be thrown and caught
    assertTrue(exceptionLatch.await(5, TimeUnit.SECONDS), "Eviction task should have completed");

    // Verify that the eviction process didn't crash the application
    verify(mockExecutor, times(1)).execute(any(Runnable.class));
    assertEquals(
        8 * 1024 * 1024L, blobStore.getMemoryUsage(), "Memory usage should not have changed");
  }
}
