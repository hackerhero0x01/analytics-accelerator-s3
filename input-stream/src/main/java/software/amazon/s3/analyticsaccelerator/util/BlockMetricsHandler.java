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

/**
 * Handles metrics updates for blocks in the Analytics Accelerator. This class provides thread-safe
 * operations for updating both blob-specific and aggregated metrics. It relies on the underlying
 * thread safety of the {@link Metrics} class for concurrent operations.
 */
public class BlockMetricsHandler {

  /** Metrics specific to individual blobs */
  private final Metrics blobMetrics;

  /** Aggregated metrics across all blobs */
  private final Metrics aggregatingMetrics;

  /**
   * Constructs a new BlockMetricsHandler.
   *
   * @param blobMetrics metrics instance for tracking blob-specific metrics
   * @param aggregatingMetrics metrics instance for tracking aggregated metrics across all blobs
   */
  public BlockMetricsHandler(Metrics blobMetrics, Metrics aggregatingMetrics) {
    this.blobMetrics = blobMetrics;
    this.aggregatingMetrics = aggregatingMetrics;
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
}
