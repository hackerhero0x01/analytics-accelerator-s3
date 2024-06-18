package com.amazon.connector.s3.io.logical.impl;

import com.amazon.connector.s3.io.logical.LogicalIO;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.parquet.FileTail;
import com.amazon.connector.s3.io.logical.parquet.ParquetMetadataTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetPrefetchRemainingColumnTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetPrefetchTailTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetReadTailTask;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.object.ObjectMetadata;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
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

  private static final Logger LOG = LogManager.getLogger(ParquetLogicalIOImpl.class);

  /**
   * Constructs an instance of LogicalIOImpl.
   *
   * @param physicalIO underlying physical IO that knows how to fetch bytes
   * @param logicalIOConfiguration configuration for this logical IO implementation
   */
  public ParquetLogicalIOImpl(
      @NonNull PhysicalIO physicalIO, @NonNull LogicalIOConfiguration logicalIOConfiguration) {
    this(
        physicalIO,
        logicalIOConfiguration,
        new ParquetPrefetchTailTask(logicalIOConfiguration, physicalIO),
        new ParquetReadTailTask(logicalIOConfiguration, physicalIO),
        new ParquetMetadataTask(logicalIOConfiguration, physicalIO),
        new ParquetPrefetchRemainingColumnTask(logicalIOConfiguration, physicalIO));
  }

  protected ParquetLogicalIOImpl(
      PhysicalIO physicalIO,
      LogicalIOConfiguration logicalIOConfiguration,
      ParquetPrefetchTailTask parquetPrefetchTailTask,
      ParquetReadTailTask parquetReadTailTask,
      ParquetMetadataTask parquetMetadataTask,
      ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask) {
    this.physicalIO = physicalIO;
    this.logicalIOConfiguration = logicalIOConfiguration;
    this.parquetPrefetchTailTask = parquetPrefetchTailTask;
    this.parquetReadTailTask = parquetReadTailTask;
    this.parquetMetadataTask = parquetMetadataTask;
    this.parquetPrefetchRemainingColumnTask = parquetPrefetchRemainingColumnTask;

    prefetchFooterAndBuildMetadata();
  }

  @Override
  public int read(long position) throws IOException {
    return physicalIO.read(position);
  }

  @Override
  public int read(byte[] buf, int off, int len, long position) throws IOException {
    if (logicalIOConfiguration.isMetadataAwarePefetchingEnabled()) {
      CompletableFuture.supplyAsync(
          () -> parquetPrefetchRemainingColumnTask.prefetchRemainingColumnChunk(position, len));
    }

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

  private void prefetchFooterAndBuildMetadata() {
    if (logicalIOConfiguration.isFooterCachingEnabled()) {
      parquetPrefetchTailTask.prefetchTail();
    }

    if (logicalIOConfiguration.isMetadataAwarePefetchingEnabled()
        && physicalIO.columnMappers() == null) {
      CompletableFuture.supplyAsync(parquetReadTailTask)
          .thenAccept((FileTail fileTail) -> parquetMetadataTask.storeColumnMappers(fileTail));
    }
  }
}
