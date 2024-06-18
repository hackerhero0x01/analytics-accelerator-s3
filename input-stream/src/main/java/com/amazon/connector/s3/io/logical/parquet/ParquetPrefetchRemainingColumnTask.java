package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.Range;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Task for prefetching the remainder of a column chunk. */
public class ParquetPrefetchRemainingColumnTask {

  private final PhysicalIO physicalIO;
  private final LogicalIOConfiguration logicalIOConfiguration;

  private static final Logger LOG = LogManager.getLogger(ParquetPrefetchRemainingColumnTask.class);

  /**
   * When a column chunk at position x is read partially, prefetch the remaining bytes of the chunk.
   *
   * @param physicalIO physicalIO instance
   * @param logicalIOConfiguration logicalIO configuration
   */
  public ParquetPrefetchRemainingColumnTask(
      @NonNull LogicalIOConfiguration logicalIOConfiguration, @NonNull PhysicalIO physicalIO) {
    this.physicalIO = physicalIO;
    this.logicalIOConfiguration = logicalIOConfiguration;
  }

  /**
   * Prefetches the remaining colum chunk.
   *
   * @param position current position of read
   * @param len length of read
   * @return ranges prefetched
   */
  public List<Range> prefetchRemainingColumnChunk(long position, int len) {
    ColumnMappers columnMappers = physicalIO.columnMappers();

    if (columnMappers != null) {
      HashMap<String, ColumnMetadata> offsetIndexToColumnMap =
          columnMappers.getOffsetIndexToColumnMap();
      if (offsetIndexToColumnMap.containsKey(Long.toString(position))) {
        return createRemainingColumnPrefetchPlan(
            offsetIndexToColumnMap.get(Long.toString(position)), position, len);
      }
    }

    return null;
  }

  private List<Range> createRemainingColumnPrefetchPlan(
      ColumnMetadata columnMetadata, long position, int len) {

    if (len < columnMetadata.getCompressedSize()) {
      long startRange = position + len;
      long endRange = startRange + (columnMetadata.getCompressedSize() - len);
      List<Range> prefetchRanges = new ArrayList<>();
      prefetchRanges.add(new Range(startRange, endRange));
      IOPlan ioPlan = IOPlan.builder().prefetchRanges(prefetchRanges).build();
      try {
        physicalIO.execute(ioPlan);
        return prefetchRanges;
      } catch (Exception e) {
        LOG.debug("Error in executing remaining column prefetch plan", e);
      }
    }

    return null;
  }
}
