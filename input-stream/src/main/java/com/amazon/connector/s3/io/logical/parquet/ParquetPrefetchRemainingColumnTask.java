package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.common.telemetry.Operation;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.logical.impl.ParquetColumnPrefetchStore;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.IOPlanExecution;
import com.amazon.connector.s3.io.physical.plan.IOPlanState;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.util.S3URI;
import com.amazon.connector.s3.util.StreamAttributes;
import java.util.concurrent.CompletionException;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Task for prefetching the remainder of a column chunk. */
public class ParquetPrefetchRemainingColumnTask {
  private final S3URI s3Uri;
  private final Telemetry telemetry;
  private final PhysicalIO physicalIO;
  private final ParquetColumnPrefetchStore parquetColumnPrefetchStore;

  private static final String OPERATION_PARQUET_PREFETCH_COLUMN_CHUNK =
      "parquet.task.prefetch.column.chunk";
  private static final Logger LOG = LogManager.getLogger(ParquetPrefetchRemainingColumnTask.class);

  /**
   * When a column chunk at position x is read partially, prefetch the remaining bytes of the chunk.
   *
   * @param s3URI the object's S3 URI
   * @param physicalIO physicalIO instance
   * @param telemetry an instance of {@link Telemetry} to use
   * @param parquetColumnPrefetchStore object containing Parquet usage information
   */
  public ParquetPrefetchRemainingColumnTask(
      @NonNull S3URI s3URI,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIO physicalIO,
      @NonNull ParquetColumnPrefetchStore parquetColumnPrefetchStore) {
    this.s3Uri = s3URI;
    this.telemetry = telemetry;
    this.physicalIO = physicalIO;
    this.parquetColumnPrefetchStore = parquetColumnPrefetchStore;
  }

  /**
   * Prefetches the remaining colum chunk.
   *
   * @param position current position of read
   * @param len length of read
   * @return ranges prefetched
   */
  public IOPlanExecution prefetchRemainingColumnChunk(long position, int len) {
    ColumnMappers columnMappers = parquetColumnPrefetchStore.getColumnMappers(s3Uri);
    if (columnMappers != null) {
      ColumnMetadata columnMetadata = columnMappers.getOffsetIndexToColumnMap().get(position);
      if (columnMetadata != null) {
        return telemetry.measureVerbose(
            () ->
                Operation.builder()
                    .name(OPERATION_PARQUET_PREFETCH_COLUMN_CHUNK)
                    .attribute(StreamAttributes.column(columnMetadata.getColumnName()))
                    .attribute(StreamAttributes.uri(this.s3Uri))
                    .attribute(StreamAttributes.range(position, position + len - 1))
                    .build(),
            () -> executeRemainingColumnPrefetchPlan(columnMetadata, position, len));
      }
    }
    return IOPlanExecution.builder().state(IOPlanState.SKIPPED).build();
  }

  /**
   * Creates and executes a prefetch plan for columns
   *
   * @param columnMetadata colum metadata
   * @param position position
   * @param len length
   * @return result of plan execution
   */
  private IOPlanExecution executeRemainingColumnPrefetchPlan(
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
