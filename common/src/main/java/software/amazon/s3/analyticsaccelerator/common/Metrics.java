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
package software.amazon.s3.analyticsaccelerator.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import software.amazon.s3.analyticsaccelerator.util.MetricKey;

/**
 * A thread-safe metrics collection class that maintains counters for different types of metrics.
 * Each metric is identified by a {@link MetricKey} and stored as an {@link AtomicLong} value.
 */
public class Metrics {

  /**
   * Thread-safe map storing metric values. Keys are {@link MetricKey} enums and values are {@link
   * AtomicLong} counters for each metric.
   */
  private final Map<MetricKey, AtomicLong> metrics = new ConcurrentHashMap<>();

  /**
   * Adds the specified delta to the metric identified by the given key. If the metric doesn't
   * exist, it will be created with an initial value of 0 before adding the delta.
   *
   * @param key the metric key to identify which metric to update
   * @param delta the value to add to the metric (can be negative for decrements)
   */
  public void add(MetricKey key, long delta) {
    getOrCreate(key).addAndGet(delta);
  }

  /**
   * Retrieves the current value of the specified metric. If the metric doesn't exist, it will be
   * created with an initial value of 0.
   *
   * @param key the metric key whose value should be retrieved
   * @return the current value of the metric
   */
  public long get(MetricKey key) {
    return getOrCreate(key).get();
  }

  /**
   * Helper method to get or create an {@link AtomicLong} for a given metric key. If the metric
   * doesn't exist, it creates a new {@link AtomicLong} initialized to 0.
   *
   * @param key the metric key to look up or create
   * @return the existing or newly created {@link AtomicLong} for the metric
   */
  private AtomicLong getOrCreate(MetricKey key) {
    return metrics.computeIfAbsent(key, k -> new AtomicLong(0));
  }
}
