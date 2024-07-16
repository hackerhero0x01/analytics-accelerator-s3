package com.amazon.connector.s3.io.physical.v2.data;

import com.amazon.connector.s3.io.physical.plan.Range;
import java.util.LinkedList;
import java.util.List;
import java.util.OptionalLong;
import lombok.RequiredArgsConstructor;

/**
 * Class responsible for implementing how to plan reads over a BlockStore. Today its main
 * responsibility is ensuring that there are no overlapping ranges in the BlockStore.
 */
@RequiredArgsConstructor
public class IOPlanner {

  private final BlockStore blockStore;

  /**
   * Given the start and end of a range, return which ranges to fetch from the object store to have
   * coverage over the whole range.
   *
   * @param pos the starting position of a read
   * @param end the end of a read
   * @param lastObjectByte the zero-indexed position of the last object byte
   * @return a list of Ranges that need to be fetched
   */
  public List<Range> planRead(long pos, long end, long lastObjectByte) {
    List<Range> missingRanges = new LinkedList<>();

    OptionalLong nextMissingByte = blockStore.findNextMissingByte(pos);

    while (nextMissingByte.isPresent()
        && nextMissingByte.getAsLong() <= Math.min(end, lastObjectByte)) {
      OptionalLong nextAvailableByte =
          blockStore.findNextAvailableByte(nextMissingByte.getAsLong());

      final long endOfRange;
      if (nextAvailableByte.isPresent()) {
        endOfRange = Math.min(end, nextAvailableByte.getAsLong() - 1);
      } else {
        endOfRange = Math.min(end, lastObjectByte);
      }

      missingRanges.add(new Range(nextMissingByte.getAsLong(), endOfRange));
      nextMissingByte = blockStore.findNextMissingByte(endOfRange + 1);
    }
    return missingRanges;
  }
}
