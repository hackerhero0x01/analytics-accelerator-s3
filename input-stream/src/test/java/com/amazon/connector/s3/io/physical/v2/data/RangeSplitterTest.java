package com.amazon.connector.s3.io.physical.v2.data;

import static com.amazon.connector.s3.util.Constants.ONE_MB;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazon.connector.s3.io.physical.plan.Range;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class RangeSplitterTest {

  @Test
  public void test__splitRanges__smallRangesCauseNoSplit() {
    // Given: small ranges
    List<Range> ranges = new LinkedList<>();
    ranges.add(new Range(0, 100));
    ranges.add(new Range(200, 300));
    ranges.add(new Range(300, 400));
    ranges.add(new Range(400, 500));

    // When: splitRanges is called
    List<Range> splitRanges = RangeSplitter.splitRanges(ranges);

    // Then: nothing happens
    assertEquals(ranges, splitRanges);
  }

  @Test
  public void test__splitRanges__bigRangesResultInSplits() {
    // Given: a 16MB range
    List<Range> ranges = new LinkedList<>();
    ranges.add(new Range(0, 16 * ONE_MB - 1));

    // When: splitRanges is called
    List<Range> splitRanges = RangeSplitter.splitRanges(ranges);

    // Then: 16MB range is split into 4x4MB ranges
    List<Range> expected = new LinkedList<>();
    expected.add(new Range(0, 4 * ONE_MB - 1));
    expected.add(new Range(4 * ONE_MB, 8 * ONE_MB - 1));
    expected.add(new Range(8 * ONE_MB, 12 * ONE_MB - 1));
    expected.add(new Range(12 * ONE_MB, 16 * ONE_MB - 1));
    assertEquals(expected, splitRanges);
  }
}
