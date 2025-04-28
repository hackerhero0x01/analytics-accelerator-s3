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
package software.amazon.s3.analyticsaccelerator.util;

import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStoreIndexCache;

/**
 * Handles metrics updates for blocks in the Analytics Accelerator. This class provides thread-safe
 * operations for updating both blob-specific and aggregated metrics. It relies on the underlying
 * thread safety of the {@link Metrics} class for concurrent operations.
 */
public class BlockMetricsAndCacheHandler {

  /** Metrics specific to individual blobs */
  private final Metrics blobMetrics;

  /** Aggregated metrics across all blobs */
  private final Metrics aggregatingMetrics;

  /** Blobstore index cache */
  private final BlobStoreIndexCache indexCache;

  /**
   * Constructs a new BlockMetricsHandler.
   *
   * @param blobMetrics metrics instance for tracking blob-specific metrics
   * @param aggregatingMetrics metrics instance for tracking aggregated metrics across all blobs
   * @param indexCache blobstore indexes
   */
  public BlockMetricsAndCacheHandler(
      Metrics blobMetrics, Metrics aggregatingMetrics, BlobStoreIndexCache indexCache) {
    this.blobMetrics = blobMetrics;
    this.aggregatingMetrics = aggregatingMetrics;
    this.indexCache = indexCache;
  }

  /**
   * Updates both blob-specific and aggregated metrics with the provided value. For memory usage
   * metrics, updates both blob-specific and aggregated metrics. For other metrics, updates only the
   * aggregated metrics. This method is thread-safe as it relies on the thread safety of the
   * underlying {@link Metrics} implementation.
   *
   * @param key the metric key to be updated
   * @param value the value to add to the metric
   */
  public void updateMetrics(MetricKey key, long value) {
    if (key.equals(MetricKey.MEMORY_USAGE)) {
      blobMetrics.add(key, value);
    }
    aggregatingMetrics.add(key, value);
  }

  /**
   * Reduces both blob-specific and aggregated metrics with the provided value. For memory usage
   * metrics, updates both blob-specific and aggregated metrics. For other metrics, updates only the
   * aggregated metrics. This method is thread-safe as it relies on the thread safety of the
   * underlying {@link Metrics} implementation.
   *
   * @param key the metric key to be updated
   * @param value the value to add to the metric
   */
  public void reduceMetrics(MetricKey key, long value) {
    if (key.equals(MetricKey.MEMORY_USAGE)) {
      blobMetrics.reduce(key, value);
    }
    aggregatingMetrics.reduce(key, value);
  }

  /**
   * Checks if a specific block key exists in the index cache.
   *
   * @param blockKey The key to check for presence in the index cache
   * @return true if the block key exists in the cache, false otherwise
   */
  public boolean isPresentInIndexCache(BlockKey blockKey) {
    return indexCache.contains(blockKey);
  }

  /**
   * Stores a block key and its associated range value in the index cache.
   *
   * @param key The block key to be stored in the cache
   * @param range The range value associated with the block key
   */
  public void putInIndexCache(BlockKey key, int range) {
    indexCache.put(key, range);
  }

  /**
   * Retrieves the value associated with the specified block key from the index cache, if it exists.
   *
   * @param key The block key whose associated value is to be retrieved
   */
  public void getIfPresentFromIndexCache(BlockKey key) {
    indexCache.getIfPresent(key);
  }
}
