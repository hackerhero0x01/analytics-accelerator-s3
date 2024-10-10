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
package software.amazon.s3.dataaccelerator.io.physical.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static software.amazon.s3.dataaccelerator.util.Constants.ONE_MB;

import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.s3.dataaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.dataaccelerator.request.Range;

public class RangeOptimiserTest {

  @Test
  public void test__splitRanges__smallRangesCauseNoSplit() {
    // Given: small ranges
    RangeOptimiser rangeOptimiser = new RangeOptimiser(PhysicalIOConfiguration.DEFAULT);
    List<Range> ranges = new LinkedList<>();
    ranges.add(new Range(0, 100));
    ranges.add(new Range(200, 300));
    ranges.add(new Range(300, 400));
    ranges.add(new Range(400, 500));

    // When: splitRanges is called
    List<Range> splitRanges = rangeOptimiser.splitRanges(ranges);

    // Then: nothing happens
    assertEquals(ranges, splitRanges);
  }

  @Test
  public void test__splitRanges__bigRangesResultInSplits() {
    // Given: a 16MB range
    RangeOptimiser rangeOptimiser = new RangeOptimiser(PhysicalIOConfiguration.DEFAULT);
    List<Range> ranges = new LinkedList<>();
    ranges.add(new Range(0, 16 * ONE_MB - 1));

    // When: splitRanges is called
    List<Range> splitRanges = rangeOptimiser.splitRanges(ranges);

    // Then: 16MB range is split into 4x4MB ranges
    List<Range> expected = new LinkedList<>();
    expected.add(new Range(0, 8 * ONE_MB - 1));
    expected.add(new Range(8 * ONE_MB, 16 * ONE_MB - 1));
    assertEquals(expected, splitRanges);
  }
}
