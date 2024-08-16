package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.impl.ParquetMetadataStore;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.IOPlanExecution;
import com.amazon.connector.s3.io.physical.plan.IOPlanState;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.util.S3URI;
import java.util.HashMap;
import java.util.concurrent.CompletionException;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Task for prefetching the remainder of a column chunk. */
public class ParquetPrefetchRemainingColumnTask {

  private final S3URI s3Uri;
  private final PhysicalIO physicalIO;
  private final ParquetMetadataStore parquetMetadataStore;
  private final LogicalIOConfiguration logicalIOConfiguration;

  private static final Logger LOG = LogManager.getLogger(ParquetPrefetchRemainingColumnTask.class);

  /**
   * When a column chunk at position x is read partially, prefetch the remaining bytes of the chunk.
   *
   * @param s3URI the object's S3 URI
   * @param physicalIO physicalIO instance
   * @param logicalIOConfiguration logicalIO configuration
   * @param parquetMetadataStore object containing Parquet usage information
   */
  public ParquetPrefetchRemainingColumnTask(
      @NonNull S3URI s3URI,
      @NonNull LogicalIOConfiguration logicalIOConfiguration,
      @NonNull PhysicalIO physicalIO,
      ParquetMetadataStore parquetMetadataStore) {
    this.s3Uri = s3URI;
    this.physicalIO = physicalIO;
    this.logicalIOConfiguration = logicalIOConfiguration;
    this.parquetMetadataStore = parquetMetadataStore;
  }

  /**
   * Prefetches the remaining colum chunk.
   *
   * @param position current position of read
   * @param len length of read
   * @return ranges prefetched
   */
  public IOPlanExecution prefetchRemainingColumnChunk(long position, int len) {
    ColumnMappers columnMappers = parquetMetadataStore.getColumnMappers(s3Uri);

    if (columnMappers != null) {
      HashMap<Long, ColumnMetadata> offsetIndexToColumnMap =
          columnMappers.getOffsetIndexToColumnMap();
      if (offsetIndexToColumnMap.containsKey(position)) {
        return createRemainingColumnPrefetchPlan(
            offsetIndexToColumnMap.get(position), position, len);
      }
    }

    return IOPlanExecution.builder().state(IOPlanState.SKIPPED).build();
  }

  private IOPlanExecution createRemainingColumnPrefetchPlan(
      ColumnMetadata columnMetadata, long position, int len) {
    if (len < columnMetadata.getCompressedSize()) {
      long startRange = position + len;
      long endRange = startRange + (columnMetadata.getCompressedSize() - len);
      IOPlan ioPlan = new IOPlan(new Range(startRange, endRange));
      try {
        return physicalIO.execute(ioPlan);
      } catch (Exception e) {
        LOG.error(
            "Error in executing remaining column chunk prefetch plan for {}. Will fallback to synchronous reading for this column.",
            this.s3Uri.getKey(),
            e);
        throw new CompletionException("Error in executing remaining column prefetch plan", e);
      }
    }

    return IOPlanExecution.builder().state(IOPlanState.SKIPPED).build();
  }
}
