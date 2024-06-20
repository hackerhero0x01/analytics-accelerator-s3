package com.amazon.connector.s3.io.logical.impl;

import com.amazon.connector.s3.io.logical.LogicalIO;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.io.logical.parquet.ParquetMetadataTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetPredictivePrefetchingTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetPrefetchRemainingColumnTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetPrefetchTailTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetReadTailTask;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.object.ObjectMetadata;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A basic proxying implementation of a LogicalIO layer. To be extended later with logical
 * optimisations (for example, reading Parquet footers and interpreting Parquet metadata).
 */
public class ParquetLogicalIOImpl implements LogicalIO {

  private final PhysicalIO physicalIO;
  private final LogicalIOConfiguration logicalIOConfiguration;
  private final ParquetMetadataTask parquetMetadataTask;
  private final ParquetPrefetchTailTask parquetPrefetchTailTask;
  private final ParquetReadTailTask parquetReadTailTask;
  private final ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask;
  private final ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask;

  private static final Logger LOG = LogManager.getLogger(ParquetLogicalIOImpl.class);

  /**
   * Constructs an instance of LogicalIOImpl.
   *
   * @param physicalIO underlying physical IO that knows how to fetch bytes
   * @param logicalIOConfiguration configuration for this logical IO implementation
   */
  public ParquetLogicalIOImpl(
      PhysicalIO physicalIO, LogicalIOConfiguration logicalIOConfiguration) {
    this(
        physicalIO,
        logicalIOConfiguration,
        new ParquetPrefetchTailTask(logicalIOConfiguration, physicalIO),
        new ParquetReadTailTask(logicalIOConfiguration, physicalIO),
        new ParquetMetadataTask(logicalIOConfiguration, physicalIO),
        new ParquetPrefetchRemainingColumnTask(logicalIOConfiguration, physicalIO),
        new ParquetPredictivePrefetchingTask(logicalIOConfiguration, physicalIO));

    prefetchFooterAndBuildMetadata();
  }

  /**
   * Constructs an instance of LogicalIOImpl. This version of the constructor allows for dependency
   * injection.
   *
   * @param physicalIO underlying physical IO that knows how to fetch bytes
   * @param logicalIOConfiguration configuration for this logical IO implementation
   * @param parquetPrefetchTailTask task for prefetching parquet file tail
   * @param parquetReadTailTask task for reading parquet file tail
   * @param parquetMetadataTask task for parsing parquet metadata
   * @param parquetPrefetchRemainingColumnTask task for prefetching remaining column task
   * @param parquetPredictivePrefetchingTask task for predictively prefetching columns
   */
  protected ParquetLogicalIOImpl(
      PhysicalIO physicalIO,
      LogicalIOConfiguration logicalIOConfiguration,
      ParquetPrefetchTailTask parquetPrefetchTailTask,
      ParquetReadTailTask parquetReadTailTask,
      ParquetMetadataTask parquetMetadataTask,
      ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask,
      ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask) {
    this.physicalIO = physicalIO;
    this.logicalIOConfiguration = logicalIOConfiguration;
    this.parquetPrefetchTailTask = parquetPrefetchTailTask;
    this.parquetReadTailTask = parquetReadTailTask;
    this.parquetMetadataTask = parquetMetadataTask;
    this.parquetPrefetchRemainingColumnTask = parquetPrefetchRemainingColumnTask;
    this.parquetPredictivePrefetchingTask = parquetPredictivePrefetchingTask;
  }

  @Override
  public int read(long position) throws IOException {
    return physicalIO.read(position);
  }

  @Override
  public int read(byte[] buf, int off, int len, long position) throws IOException {
    prefetchRemainingColumnChunk(position, len);
    parquetPredictivePrefetchingTask.addToRecentColumnList(position);
    return physicalIO.read(buf, off, len, position);
  }

  @Override
  public int readTail(byte[] buf, int off, int len) throws IOException {
    return physicalIO.readTail(buf, off, len);
  }

  @Override
  public CompletableFuture<ObjectMetadata> metadata() {
    return physicalIO.metadata();
  }

  @Override
  public void close() throws IOException {
    physicalIO.close();
  }

  protected Optional<CompletableFuture<Optional<List<Range>>>> prefetchRemainingColumnChunk(
      long position, int len) {
    if (logicalIOConfiguration.isMetadataAwarePefetchingEnabled()
        && !logicalIOConfiguration.isPredictivePrefetchingEnabled()) {
      return Optional.of(
          CompletableFuture.supplyAsync(
              () ->
                  parquetPrefetchRemainingColumnTask.prefetchRemainingColumnChunk(position, len)));
    }

    return Optional.empty();
  }

  protected Optional<CompletableFuture<Optional<ColumnMappers>>> prefetchFooterAndBuildMetadata() {
    if (logicalIOConfiguration.isFooterCachingEnabled()) {
      parquetPrefetchTailTask.prefetchTail();
    }

    if (physicalIO.columnMappers() == null) {
      if (logicalIOConfiguration.isMetadataAwarePefetchingEnabled()
          || logicalIOConfiguration.isPredictivePrefetchingEnabled()) {
        CompletableFuture<Optional<ColumnMappers>> columnMappersCompletableFuture =
            CompletableFuture.supplyAsync(() -> parquetReadTailTask.readFileTail())
                .thenApply(parquetMetadataTask::storeColumnMappers);

        prefetchPredictedColumns(columnMappersCompletableFuture);
        return Optional.of(columnMappersCompletableFuture);
      }
    }

    return Optional.empty();
  }

  protected Optional<CompletableFuture<Optional<List<Range>>>> prefetchPredictedColumns(
      CompletableFuture<Optional<ColumnMappers>> columnMappersCompletableFuture) {
    if (logicalIOConfiguration.isPredictivePrefetchingEnabled()) {
      return Optional.of(
          columnMappersCompletableFuture.thenApply(
              parquetPredictivePrefetchingTask::prefetchRecentColumns));
    }

    return Optional.empty();
  }
}
