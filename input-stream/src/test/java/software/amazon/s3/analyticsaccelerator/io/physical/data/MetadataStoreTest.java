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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.TestTelemetry;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.RequestCallback;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class MetadataStoreTest {

  @Test
  public void test__get__cacheWorks() throws IOException {
    // Given: a MetadataStore with caching turned on
    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata objectMetadata = ObjectMetadata.builder().etag("random").build();
    when(objectClient.headObject(any(), any())).thenReturn(objectMetadata);
    MetadataStore metadataStore =
        new MetadataStore(
            objectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    S3URI key = S3URI.of("foo", "bar");

    // When: get(..) is called multiple times
    metadataStore.get(key, OpenStreamInformation.DEFAULT);
    metadataStore.get(key, OpenStreamInformation.DEFAULT);
    metadataStore.get(key, OpenStreamInformation.DEFAULT);

    // Then: object store was accessed only once
    verify(objectClient, times(1)).headObject(any(), any());
  }

  @Test
  void testEvictKey_ExistingKey() throws IOException {
    // Setup
    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.headObject(any(), any())).thenReturn(mock(ObjectMetadata.class));
    MetadataStore metadataStore =
        new MetadataStore(
            objectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    S3URI key = S3URI.of("foo", "bar");
    metadataStore.storeObjectMetadata(key, ObjectMetadata.builder().etag("random").build());

    // Test
    boolean result = metadataStore.evictKey(key);

    // Verify
    assertTrue(result, "Evicting existing key should return true");
    result = metadataStore.evictKey(key);
    assertFalse(result, "Evicting existing key should return false");
  }

  @Test
  public void testHeadRequestCallbackCalled() throws IOException {
    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata objectMetadata = ObjectMetadata.builder().etag("random").build();
    when(objectClient.headObject(any(), any())).thenReturn(objectMetadata);
    MetadataStore metadataStore =
        new MetadataStore(
            objectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));

    RequestCallback mockCallback = mock(RequestCallback.class);
    OpenStreamInformation openStreamInfo =
        OpenStreamInformation.builder().requestCallback(mockCallback).build();

    S3URI s3URI = S3URI.of("bucket", "key");

    metadataStore.get(s3URI, openStreamInfo);
    verify(mockCallback, times(1)).onHeadRequest();
  }

  @Test
  public void testTtlBasedEviction() throws IOException, InterruptedException {
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().metadataCacheTtlMilliseconds(1).build();

    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata objectMetadata = ObjectMetadata.builder().etag("random").build();
    when(objectClient.headObject(any(), any())).thenReturn(objectMetadata);

    MetadataStore metadataStore =
        new MetadataStore(objectClient, TestTelemetry.DEFAULT, config, mock(Metrics.class));
    S3URI key = S3URI.of("foo", "bar");

    // Get entry, wait for TTL expiry, then get again
    metadataStore.get(key, OpenStreamInformation.DEFAULT);
    Thread.sleep(3); // Wait for TTL expiry
    metadataStore.get(key, OpenStreamInformation.DEFAULT);

    // Object store was accessed twice due to TTL expiry
    verify(objectClient, times(2)).headObject(any(), any());
  }

  @Test
  public void testMetadataTtlProvidesNewVersionAfterExpiry()
      throws IOException, InterruptedException {
    // Test that TTL expiry allows getting updated object versions
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().metadataCacheTtlMilliseconds(20).build();

    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata oldVersion =
        ObjectMetadata.builder().etag("old-etag").contentLength(100).build();
    ObjectMetadata newVersion =
        ObjectMetadata.builder().etag("new-etag").contentLength(200).build();

    when(objectClient.headObject(any(), any())).thenReturn(oldVersion).thenReturn(newVersion);

    MetadataStore metadataStore =
        new MetadataStore(objectClient, TestTelemetry.DEFAULT, config, mock(Metrics.class));
    S3URI key = S3URI.of("bucket", "updated-object");

    // Get old version of object
    ObjectMetadata first = metadataStore.get(key, OpenStreamInformation.DEFAULT);
    assertEquals("old-etag", first.getEtag());
    assertEquals(100, first.getContentLength());

    Thread.sleep(30); // Wait for TTL expiry

    // Get new version of object
    ObjectMetadata second = metadataStore.get(key, OpenStreamInformation.DEFAULT);
    assertEquals("new-etag", second.getEtag());
    assertEquals(200, second.getContentLength());

    verify(objectClient, times(2)).headObject(any(), any());
  }

  @Test
  public void testZeroTtlDisablesMetadataCache() throws IOException {
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().metadataCacheTtlMilliseconds(0).build();

    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata originalMetadata = ObjectMetadata.builder().etag("original-etag").build();
    ObjectMetadata updatedMetadata = ObjectMetadata.builder().etag("updated-etag").build();

    when(objectClient.headObject(any(), any()))
        .thenReturn(originalMetadata)
        .thenReturn(originalMetadata)
        .thenReturn(updatedMetadata);

    MetadataStore metadataStore =
        new MetadataStore(objectClient, TestTelemetry.DEFAULT, config, mock(Metrics.class));
    S3URI key = S3URI.of("bucket", "key");

    // Create first 2 streams - each should get original etag
    ObjectMetadata stream1 = metadataStore.get(key, OpenStreamInformation.DEFAULT);
    ObjectMetadata stream2 = metadataStore.get(key, OpenStreamInformation.DEFAULT);

    assertEquals("original-etag", stream1.getEtag());
    assertEquals("original-etag", stream2.getEtag());

    // 3rd stream should get updated etag
    ObjectMetadata stream3 = metadataStore.get(key, OpenStreamInformation.DEFAULT);
    assertEquals("updated-etag", stream3.getEtag());

    verify(objectClient, times(3)).headObject(any(), any());
  }

  @Test
  public void testStoreObjectMetadataRefreshesTtlForExistingKey()
      throws IOException, InterruptedException {
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder().metadataCacheTtlMilliseconds(100).build();

    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata metadata = ObjectMetadata.builder().etag("test-etag").build();
    when(objectClient.headObject(any(), any())).thenReturn(metadata);

    MetadataStore metadataStore =
        new MetadataStore(objectClient, TestTelemetry.DEFAULT, config, mock(Metrics.class));
    S3URI key = S3URI.of("bucket", "key");

    // First get to populate cache
    metadataStore.get(key, OpenStreamInformation.DEFAULT);
    verify(objectClient, times(1)).headObject(any(), any());

    // Wait partial TTL, then store same key(should refresh TTL)
    Thread.sleep(60);
    metadataStore.storeObjectMetadata(key, metadata);

    // Wait remaining time, should still be cached due to TTL refresh
    Thread.sleep(70); // Total 130ms , but TTL was expired after 100ms but got refreshed at 60ms
    ObjectMetadata retrieved = metadataStore.get(key, OpenStreamInformation.DEFAULT);

    // Should not trigger new HEAD request, still cached due to TTL refresh
    assertEquals("test-etag", retrieved.getEtag());
    verify(objectClient, times(1)).headObject(any(), any());
  }

  @Test
  public void testConcurrentAccessOnlyMakesOneRequest()
      // Test that concurrent access to same key only makes one HEAD request due to Caffeine thread
      // safety
      throws Exception {
    ObjectClient objectClient = mock(ObjectClient.class);
    ObjectMetadata metadata = ObjectMetadata.builder().etag("concurrent-etag").build();

    when(objectClient.headObject(any(), any())).thenReturn(metadata);

    MetadataStore metadataStore =
        new MetadataStore(
            objectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT,
            mock(Metrics.class));
    S3URI key = S3URI.of("bucket", "concurrent-key");

    ExecutorService threadPool = Executors.newFixedThreadPool(5);

    // Submit 3 concurrent metadata get operations
    Future<?> task1 =
        threadPool.submit(
            () -> {
              try {
                ObjectMetadata result = metadataStore.get(key, OpenStreamInformation.DEFAULT);
                assertEquals("concurrent-etag", result.getEtag());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    Future<?> task2 =
        threadPool.submit(
            () -> {
              try {
                ObjectMetadata result = metadataStore.get(key, OpenStreamInformation.DEFAULT);
                assertEquals("concurrent-etag", result.getEtag());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    Future<?> task3 =
        threadPool.submit(
            () -> {
              try {
                ObjectMetadata result = metadataStore.get(key, OpenStreamInformation.DEFAULT);
                assertEquals("concurrent-etag", result.getEtag());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });

    task1.get();
    task2.get();
    task3.get();

    verify(objectClient, times(1)).headObject(any(), any());
    threadPool.shutdown();
  }
}
