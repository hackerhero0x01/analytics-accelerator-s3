package com.amazon.connector.s3.io.physical.data;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.util.S3URI;
import java.io.Closeable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Class responsible for fetching and potentially caching object metadata. */
public class MetadataStore implements Closeable {

  private final ObjectClient objectClient;
  private final Map<S3URI, CompletableFuture<ObjectMetadata>> cache;

  private static final Logger LOG = LogManager.getLogger(MetadataStore.class);

  /**
   * Constructs a new MetadataStore.
   *
   * @param objectClient the object client to use for object store interactions
   * @param configuration a configuration of PhysicalIO
   */
  public MetadataStore(ObjectClient objectClient, PhysicalIOConfiguration configuration) {
    Preconditions.checkNotNull(objectClient, "`objectClient` must not be null");
    Preconditions.checkNotNull(configuration, "`configuration` must not be null");

    this.objectClient = objectClient;
    this.cache =
        Collections.synchronizedMap(
            new LinkedHashMap<S3URI, CompletableFuture<ObjectMetadata>>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry eldest) {
                return this.size() > configuration.getMetadataStoreCapacity();
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
    return this.cache.computeIfAbsent(
        s3URI,
        uri ->
            objectClient.headObject(
                HeadRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build()));
  }

  private void safeClose(CompletableFuture<ObjectMetadata> future) {
    if (!future.isDone()) {
      try {
        future.cancel(false);
      } catch (Exception e) {
        LOG.error("Error cancelling ObjectMetadata future", e);
      }
    }
  }

  @Override
  public void close() {
    this.cache.forEach((k, v) -> safeClose(v));
  }
}
