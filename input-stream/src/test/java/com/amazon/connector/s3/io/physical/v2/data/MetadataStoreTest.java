package com.amazon.connector.s3.io.physical.v2.data;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.S3URI;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class MetadataStoreTest {

  @Test
  public void test__get__cacheWorks() {
    // Given: a MetadataStore with caching turned on
    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.headObject(any()))
        .thenReturn(CompletableFuture.completedFuture(mock(ObjectMetadata.class)));
    MetadataStore metadataStore = new MetadataStore(objectClient);
    S3URI key = S3URI.of("foo", "bar");

    // When: get(..) is called multiple times
    metadataStore.get(key);
    metadataStore.get(key);
    metadataStore.get(key);

    // Then: object store was accessed only once
    verify(objectClient, times(1)).headObject(any());
  }

  @Test
  public void test__close__clearsCache() {
    // Given: a MetadataStore with caching turned on
    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.headObject(any()))
        .thenReturn(CompletableFuture.completedFuture(mock(ObjectMetadata.class)));
    MetadataStore metadataStore = new MetadataStore(objectClient);
    S3URI key = S3URI.of("foo", "bar");

    // When: a key is cached, but the cache is closed and the key is requested again
    metadataStore.get(key);
    metadataStore.close();
    metadataStore.get(key);

    // Then: key was fetched again
    verify(objectClient, times(2)).headObject(any());
  }
}
