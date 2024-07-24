package com.amazon.connector.s3.io.physical.data;

import static com.amazon.connector.s3.util.Constants.ONE_MB;

import com.amazon.connector.s3.io.physical.plan.Range;
import java.util.LinkedList;
import java.util.List;

/**
 * RangeSplitter is responsible for splitting up big ranges into smaller reads. The need for such
 * functionality arises from sequential prefetching. When we decide that, e.g., the next 128MB chunk
 * of an object is needed with high confidence, then we should not fetch this in a single request.
 *
 * <p>This class is capable of implementing heuristics on how to fetch ranges of different sizes
 * optimally.
 */
public class RangeSplitter {

  // TODO: these should come from configurations
  private static final long SPLITBACK_THRESHOLD = 8 * ONE_MB;

  /**
   * Given a list of ranges, return a potentially new set of ranges which is more optimal to fetch
   * (i.e., split up huge ranges based on a heuristic).
   *
   * @param ranges a list of ranges
   * @return a potentially different list of ranges with big ranges split up
   */
  public static List<Range> splitRanges(List<Range> ranges) {
    List<Range> splits = new LinkedList<>();

    for (Range range : ranges) {
      if (range.getLength() > SPLITBACK_THRESHOLD) {
        splitRange(range.getStart(), range.getEnd()).forEach(splits::add);
      } else {
        splits.add(range);
      }
    }

    return splits;
  }

  private static List<Range> splitRange(long start, long end) {
    long nextRangeStart = start;
    List<Range> generatedRanges = new LinkedList<>();

    int splitBackIndex = 0;
    while (nextRangeStart < end) {
      long rangeEnd = Math.min(nextRangeStart + getSplitbackBlockSize(splitBackIndex) - 1, end);
      generatedRanges.add(new Range(nextRangeStart, rangeEnd));
      nextRangeStart = rangeEnd + 1;
      ++splitBackIndex;
    }

    return generatedRanges;
  }

  private static long getSplitbackBlockSize(int index) {
    // TODO: we should fine tune this
    // Progression is: 4,4,4,4,8,8,8,8,16,16,16,16...
    return 4 * ONE_MB;
  }
}
