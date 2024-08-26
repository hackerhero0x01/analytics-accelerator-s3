package com.amazon.connector.s3.io.physical.data;

import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.request.Range;
import java.util.LinkedList;
import java.util.List;
import lombok.Value;

/**
 * RangeSplitter is responsible for splitting up big ranges into smaller reads. The need for such
 * functionality arises from sequential prefetching. When we decide that, e.g., the next 128MB chunk
 * of an object is needed with high confidence, then we should not fetch this in a single request.
 *
 * <p>This class is capable of implementing heuristics on how to fetch ranges of different sizes
 * optimally.
 */
@Value
public class RangeOptimiser {
  PhysicalIOConfiguration configuration;

  /**
   * Given a list of ranges, return a potentially new set of ranges which is more optimal to fetch
   * (i.e., split up huge ranges based on a heuristic).
   *
   * @param ranges a list of ranges
   * @return a potentially different list of ranges with big ranges split up
   */
  public List<Range> splitRanges(List<Range> ranges) {
    List<Range> splits = new LinkedList<>();
    for (Range range : ranges) {
      if (range.getLength() > configuration.getMaxRangeSizeBytes()) {
        splitRange(range.getStart(), range.getEnd()).forEach(splits::add);
      } else {
        splits.add(range);
      }
    }

    return splits;
  }

  private List<Range> splitRange(long start, long end) {
    long nextRangeStart = start;
    List<Range> generatedRanges = new LinkedList<>();

    while (nextRangeStart < end) {
      long rangeEnd = Math.min(nextRangeStart + configuration.getPartSizeBytes() - 1, end);
      generatedRanges.add(new Range(nextRangeStart, rangeEnd));
      nextRangeStart = rangeEnd + 1;
    }

    return generatedRanges;
  }
}
