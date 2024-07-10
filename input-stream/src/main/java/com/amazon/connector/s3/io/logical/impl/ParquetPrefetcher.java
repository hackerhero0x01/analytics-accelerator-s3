package com.amazon.connector.s3.io.logical.impl;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.io.logical.parquet.ParquetMetadataParsingTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetPredictivePrefetchingTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetPrefetchRemainingColumnTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetPrefetchTailTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetReadTailTask;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlanExecution;
import com.amazon.connector.s3.io.physical.plan.IOPlanState;
import com.amazon.connector.s3.util.S3URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

/**
 * A Parquet prefetcher is a common place for all Parquet-related async prefetching activity
 * (prefetching and caching footers, parsing and interpreting footers, collecting Parquet usage
 * information and doing prefetching based on them).
 *
 * <p>The Parquet prefetcher swallows all exceptions arising from the tasks it schedules because
 * exceptions do not escape CompletableFutures.
 */
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ParquetPrefetcher {

  private final S3URI s3URI;

  // Configuration
  private final LogicalIOConfiguration logicalIOConfiguration;

  // Dependencies
  private final PhysicalIO physicalIO;
  private final ParquetMetadataStore parquetMetadataStore;

  // Tasks
  private final ParquetMetadataParsingTask parquetMetadataParsingTask;
  private final ParquetPrefetchTailTask parquetPrefetchTailTask;
  private final ParquetReadTailTask parquetReadTailTask;
  private final ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask;
  private final ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask;
  private final ExecutorService asyncProcessingPool;

  /**
   * Constructs a ParquetPrefetcher.
   *
   * @param s3Uri the S3Uri of the underlying object
   * @param physicalIO the PhysicalIO capable of actually fetching the physical bytes from the
   *     object store
   * @param logicalIOConfiguration the LogicalIO's configuration
   * @param parquetMetadataStore a common place for Parquet usage information
   * @param asyncParsingPool Custom thread pool for async processing
   */
  public ParquetPrefetcher(
      S3URI s3Uri,
      PhysicalIO physicalIO,
      LogicalIOConfiguration logicalIOConfiguration,
      ParquetMetadataStore parquetMetadataStore,
      ExecutorService asyncParsingPool) {
    this(
        s3Uri,
        logicalIOConfiguration,
        physicalIO,
        parquetMetadataStore,
        new ParquetMetadataParsingTask(
            s3Uri, logicalIOConfiguration, parquetMetadataStore, asyncParsingPool),
        new ParquetPrefetchTailTask(s3Uri, logicalIOConfiguration, physicalIO),
        new ParquetReadTailTask(s3Uri, logicalIOConfiguration, physicalIO),
        new ParquetPrefetchRemainingColumnTask(
            s3Uri, logicalIOConfiguration, physicalIO, parquetMetadataStore),
        new ParquetPredictivePrefetchingTask(
            s3Uri, logicalIOConfiguration, physicalIO, parquetMetadataStore),
        asyncParsingPool);
  }

  /**
   * Given a position and length, prefetches the remaining part of the Parquet column.
   *
   * @param position a position of a read
   * @param len the length of a read
   * @return the IOPlanExecution object of the read that was pushed down to the PhysicalIO as a
   *     result of this call
   */
  public CompletableFuture<IOPlanExecution> prefetchRemainingColumnChunk(long position, int len) {
    if (logicalIOConfiguration.isMetadataAwarePrefetchingEnabled()
        && !logicalIOConfiguration.isPredictivePrefetchingEnabled()) {
      return CompletableFuture.supplyAsync(
          () -> parquetPrefetchRemainingColumnTask.prefetchRemainingColumnChunk(position, len),
          asyncProcessingPool);
    }

    return CompletableFuture.completedFuture(
        IOPlanExecution.builder().state(IOPlanState.SKIPPED).build());
  }

  /**
   * Prefetch the footer and Parquet metadata for the object that s3Uri points to
   *
   * @return the IOPlanExecution object of the read that was pushed down to the PhysicalIO as a
   *     result of this call
   */
  public CompletableFuture<IOPlanExecution> prefetchFooterAndBuildMetadata() {
    if (logicalIOConfiguration.isFooterCachingEnabled()) {
      parquetPrefetchTailTask.prefetchTail();
    }

    if (shouldPrefetch()) {
      CompletableFuture<ColumnMappers> columnMappersCompletableFuture =
          CompletableFuture.supplyAsync(parquetReadTailTask::readFileTail, asyncProcessingPool)
              .thenApply(parquetMetadataParsingTask::storeColumnMappers);

      return prefetchPredictedColumns(columnMappersCompletableFuture);
    }

    return CompletableFuture.completedFuture(
        IOPlanExecution.builder().state(IOPlanState.SKIPPED).build());
  }

  private CompletableFuture<IOPlanExecution> prefetchPredictedColumns(
      CompletableFuture<ColumnMappers> columnMappersCompletableFuture) {
    if (logicalIOConfiguration.isPredictivePrefetchingEnabled()) {
      return columnMappersCompletableFuture.thenApply(
          parquetPredictivePrefetchingTask::prefetchRecentColumns);
    }

    return CompletableFuture.completedFuture(
        IOPlanExecution.builder().state(IOPlanState.SKIPPED).build());
  }

  /**
   * Record this position in the recent column list
   *
   * @param position the position to record
   */
  public void addToRecentColumnList(long position) {
    this.parquetPredictivePrefetchingTask.addToRecentColumnList(position);
  }

  private boolean shouldPrefetch() {
    return parquetMetadataStore.getColumnMappers(s3URI) == null
        && (logicalIOConfiguration.isMetadataAwarePrefetchingEnabled()
            || logicalIOConfiguration.isPredictivePrefetchingEnabled());
  }
}
