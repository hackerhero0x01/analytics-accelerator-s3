package com.amazon.connector.s3.io.physical.data;

import com.amazon.connector.s3.common.telemetry.Operation;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.request.ObjectClient;
import com.amazon.connector.s3.request.ObjectMetadata;
import com.amazon.connector.s3.util.S3URI;
import com.amazon.connector.s3.util.StreamAttributes;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Class responsible for fetching and potentially caching object metadata. */
@SuppressFBWarnings(
    value = "SIC_INNER_SHOULD_BE_STATIC_ANON",
    justification =
        "Inner class is created very infrequently, and fluency justifies the extra pointer")
public class MetadataStore implements Closeable {
  private final ObjectClient objectClient;
  private final Telemetry telemetry;
  private final Map<S3URI, CompletableFuture<ObjectMetadata>> cache;

  private static final Logger LOG = LogManager.getLogger(MetadataStore.class);
  private static final String OPERATION_METADATA_HEAD_ASYNC = "metadata.store.head.async";
  private static final String OPERATION_METADATA_HEAD_JOIN = "metadata.store.head.join";

  /**
   * Constructs a new MetadataStore.
   *
   * @param objectClient the object client to use for object store interactions.
   * @param telemetry The {@link Telemetry} to use to report measurements.
   * @param configuration a configuration of PhysicalIO.
   */
  public MetadataStore(
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration configuration) {
    this.objectClient = objectClient;
    this.telemetry = telemetry;
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
   * Get the metadata for an object synchronously (either from cache or the underlying object
   * store).
   *
   * @param s3URI the object to fetch the metadata for
   * @return returns the {@link ObjectMetadata}.
   */
  public ObjectMetadata get(S3URI s3URI) {
    return telemetry.measureJoinCritical(
        () ->
            Operation.builder()
                .name(OPERATION_METADATA_HEAD_JOIN)
                .attribute(StreamAttributes.uri(s3URI))
                .build(),
        this.asyncGet(s3URI));
  }

  /**
   * Get the metadata for an object asynchronously (either from cache or the underlying object
   * store).
   *
   * @param s3URI the object to fetch the metadata for
   * @return returns the {@link CompletableFuture} that holds object's metadata.
   */
  public synchronized CompletableFuture<ObjectMetadata> asyncGet(S3URI s3URI) {
    return this.cache.computeIfAbsent(
        s3URI,
        uri ->
            telemetry.measureCritical(
                () ->
                    Operation.builder()
                        .name(OPERATION_METADATA_HEAD_ASYNC)
                        .attribute(StreamAttributes.uri(s3URI))
                        .build(),
                objectClient.headObject(
                    HeadRequest.builder().bucket(uri.getBucket()).key(uri.getKey()).build())));
  }

  /**
   * Utility method that cancels a {@link CompletableFuture} ignoring any exceptions.
   *
   * @param future an instance of {@link CompletableFuture} to cancel
   */
  private void safeCancel(CompletableFuture<ObjectMetadata> future) {
    if (!future.isDone()) {
      try {
        future.cancel(false);
      } catch (Exception e) {
        LOG.error("Error cancelling ObjectMetadata future", e);
      }
    }
  }

  /** Closes the {@link MetadataStore} and frees up all resources it holds. */
  @Override
  public void close() {
    this.cache.values().forEach(this::safeCancel);
  }
}
