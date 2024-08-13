package com.amazon.connector.s3.io.physical.data;

import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.common.telemetry.Operation;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.util.S3URI;
import com.amazon.connector.s3.util.StreamAttributes;
import java.util.LinkedList;
import java.util.List;
import java.util.OptionalLong;
import lombok.NonNull;

/**
 * Class responsible for implementing how to plan reads over a BlockStore. Today its main
 * responsibility is ensuring that there are no overlapping ranges in the BlockStore.
 */
public class IOPlanner {
  private final S3URI s3URI;
  private final BlockStore blockStore;
  private final Telemetry telemetry;

  private static final String OPERATION_PLAN_READ = "io.planner.read";

  /**
   * Creates a new instance of {@link IOPlanner}.
   *
   * @param s3URI the S3 URI of the object
   * @param blockStore the {@link BlobStore} to use
   * @param telemetry the {@link Telemetry} to use
   */
  public IOPlanner(
      @NonNull S3URI s3URI, @NonNull BlockStore blockStore, @NonNull Telemetry telemetry) {
    this.s3URI = s3URI;
    this.blockStore = blockStore;
    this.telemetry = telemetry;
  }

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
    Preconditions.checkArgument(0 <= pos, "`pos` must be non-negative");
    Preconditions.checkArgument(pos <= end, "`pos` must be less than or equal to `end`");
    Preconditions.checkArgument(
        pos <= lastObjectByte, "`pos` must be less than or equal to `lastObjectByte`");

    return telemetry.measure(
        Operation.builder()
            .name(OPERATION_PLAN_READ)
            .attribute(StreamAttributes.uri(this.s3URI))
            .attribute(StreamAttributes.position(pos))
            .attribute(StreamAttributes.end(end))
            .build(),
        () -> {
          List<Range> missingRanges = new LinkedList<>();

          OptionalLong nextMissingByte = blockStore.findNextMissingByte(pos);

          while (nextMissingByte.isPresent()
              && nextMissingByte.getAsLong() <= Math.min(end, lastObjectByte)) {
            OptionalLong nextAvailableByte =
                blockStore.findNextLoadedByte(nextMissingByte.getAsLong());

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
        });
  }
}
