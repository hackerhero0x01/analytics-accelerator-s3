package com.amazon.connector.s3.io.physical.v2.data;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.util.S3URI;
import java.io.Closeable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Class responsible for fetching and potentially caching object metadata. */
public class MetadataStore implements Closeable {

  private final ObjectClient objectClient;
  private final Map<S3URI, CompletableFuture<ObjectMetadata>> cache;

  /**
   * Constructs a new MetadataStore.
   *
   * @param objectClient the object client to use for object store interactions
   */
  public MetadataStore(ObjectClient objectClient) {
    this.objectClient = objectClient;
    this.cache =
        Collections.synchronizedMap(
            new LinkedHashMap<S3URI, CompletableFuture<ObjectMetadata>>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry eldest) {
                // TODO: limit should come from configuration
                return this.size() > 50;
              }
            });
  }

  /**
   * Get the metadata for an object (either from cache or the underlying object store).
   *
   * @param s3URI the object to fetch the metadata for
   * @return returns the object's metadata.
   */
  public synchronized CompletableFuture<ObjectMetadata> get(S3URI s3URI) {
    Preconditions.checkNotNull(this.cache, "the cache must not be null");

    return this.cache.computeIfAbsent(
        s3URI,
        s3URI1 ->
            objectClient.headObject(
                HeadRequest.builder().bucket(s3URI.getBucket()).key(s3URI.getKey()).build()));
  }

  @Override
  public void close() {
    this.cache.forEach((k, v) -> v.cancel(false));
    this.cache.clear();
  }
}
