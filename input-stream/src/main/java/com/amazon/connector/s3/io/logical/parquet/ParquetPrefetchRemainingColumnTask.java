package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.Range;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Task for prefetching the remainder of a column chunk. */
public class ParquetPrefetchRemainingColumnTask implements Supplier<Void> {

  private final PhysicalIO physicalIO;
  private final LogicalIOConfiguration logicalIOConfiguration;
  private final long position;
  private final int len;

  private static final Logger LOG = LogManager.getLogger(ParquetPrefetchRemainingColumnTask.class);

  /**
   * When a column chunk at position x is read partially, prefetch the remaining bytes of the chunk.
   *
   * @param physicalIO physicalIO instance
   * @param logicalIOConfiguration logicalIO configuration
   * @param position the current position of the read
   * @param len the lenght of the read
   */
  public ParquetPrefetchRemainingColumnTask(
      @NonNull PhysicalIO physicalIO,
      @NonNull LogicalIOConfiguration logicalIOConfiguration,
      long position,
      int len) {
    this.physicalIO = physicalIO;
    this.logicalIOConfiguration = logicalIOConfiguration;
    this.position = position;
    this.len = len;
  }

  @Override
  public Void get() {
    ColumnMappers columnMappers = physicalIO.columnMappers();

    if (columnMappers != null) {
      HashMap<String, ColumnMetadata> offsetIndexToColumnMap =
          columnMappers.getOffsetIndexToColumnMap();
      if (offsetIndexToColumnMap.containsKey(Long.toString(position))) {
        createRemainingColumnPrefetchPlan(
            offsetIndexToColumnMap.get(Long.toString(position)), position, len);
      }
    }

    return null;
  }

  private void createRemainingColumnPrefetchPlan(
      ColumnMetadata columnMetadata, long position, int len) {

    if (len < columnMetadata.getCompressedSize()) {
      long startRange = position + len;
      long endRange = startRange + (columnMetadata.getCompressedSize() - len);
      List<Range> prefetchRanges = new ArrayList<>();
      prefetchRanges.add(new Range(startRange, endRange));
      IOPlan ioPlan = IOPlan.builder().prefetchRanges(prefetchRanges).build();
      try {
        physicalIO.execute(ioPlan);
      } catch (Exception e) {
        LOG.debug("Error in executing remaining column prefetch plan", e);
      }
    }
  }
}
