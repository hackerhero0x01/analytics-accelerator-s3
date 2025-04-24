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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class MetricComputationUtilsTest {

  private static final double DELTA = 0.0001; // Delta for comparing double values

  @Test
  public void testComputeCacheHitRate_BothZero() {
    double result = MetricComputationUtils.computeCacheHitRate(0, 0);
    assertEquals(0.0, result, DELTA);
  }

  @Test
  public void testComputeCacheHitRate_AllHits() {
    double result = MetricComputationUtils.computeCacheHitRate(100, 0);
    assertEquals(100.0, result, DELTA);
  }

  @Test
  public void testComputeCacheHitRate_AllMisses() {
    double result = MetricComputationUtils.computeCacheHitRate(0, 100);
    assertEquals(0.0, result, DELTA);
  }

  @Test
  public void testComputeCacheHitRate_MixedHitsAndMisses() {
    double result = MetricComputationUtils.computeCacheHitRate(75, 25);
    assertEquals(75.0, result, DELTA);
  }

  @Test
  public void testComputeCacheHitRate_LargeNumbers() {
    double result = MetricComputationUtils.computeCacheHitRate(1000000, 1000000);
    assertEquals(50.0, result, DELTA);
  }

  @Test
  public void testComputeCacheHitRate_SingleHit() {
    double result = MetricComputationUtils.computeCacheHitRate(1, 0);
    assertEquals(100.0, result, DELTA);
  }

  @Test
  public void testComputeCacheHitRate_SingleMiss() {
    double result = MetricComputationUtils.computeCacheHitRate(0, 1);
    assertEquals(0.0, result, DELTA);
  }
}
