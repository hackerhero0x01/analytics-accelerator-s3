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

/** Utility class for computing various metrics related to cache performance. */
public final class MetricComputationUtils {

  /** Private constructor to prevent instantiation of utility class. */
  private MetricComputationUtils() {
    throw new AssertionError("No instances of MetricComputationUtils should be created");
  }

  /**
   * Computes the cache hit rate as a percentage based on the number of cache hits and misses. The
   * hit rate is calculated as (cache hits / (cache hits + cache misses)) * 100. If there are no
   * hits and misses (total is zero), returns 0.
   *
   * @param cacheHits The number of cache hits
   * @param cacheMiss The number of cache misses
   * @return The cache hit rate as a percentage (0-100)
   */
  public static double computeCacheHitRate(long cacheHits, long cacheMiss) {
    long hitsAndMiss = cacheHits + cacheMiss;
    double hitRate = hitsAndMiss == 0 ? 0 : (double) cacheHits / hitsAndMiss;
    return hitRate * 100;
  }
}
