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
package software.amazon.s3.dataaccelerator.io.logical.parquet;

import java.util.concurrent.CompletionException;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.dataaccelerator.common.telemetry.Operation;
import software.amazon.s3.dataaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.dataaccelerator.io.logical.impl.ParquetColumnPrefetchStore;
import software.amazon.s3.dataaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.dataaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.dataaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.dataaccelerator.io.physical.plan.IOPlanState;
import software.amazon.s3.dataaccelerator.request.Range;
import software.amazon.s3.dataaccelerator.util.S3URI;
import software.amazon.s3.dataaccelerator.util.StreamAttributes;

/** Task for prefetching the remainder of a column chunk. */
public class ParquetPrefetchRemainingColumnTask {
  private final S3URI s3Uri;
  private final Telemetry telemetry;
  private final PhysicalIO physicalIO;
  private final ParquetColumnPrefetchStore parquetColumnPrefetchStore;

  private static final String OPERATION_PARQUET_PREFETCH_COLUMN_CHUNK =
      "parquet.task.prefetch.column.chunk";
  private static final Logger LOG =
      LoggerFactory.getLogger(ParquetPrefetchRemainingColumnTask.class);

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
