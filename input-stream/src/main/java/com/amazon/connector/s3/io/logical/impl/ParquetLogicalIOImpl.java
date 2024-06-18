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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A basic proxying implementation of a LogicalIO layer. To be extended later with logical
 * optimisations (for example, reading Parquet footers and interpreting Parquet metadata).
 */
public class ParquetLogicalIOImpl implements LogicalIO {

  private final PhysicalIO physicalIO;
  private final LogicalIOConfiguration logicalIOConfiguration;

  private static final Logger LOG = LogManager.getLogger(ParquetLogicalIOImpl.class);

  /**
   * Constructs an instance of LogicalIOImpl.
   *
   * @param physicalIO underlying physical IO that knows how to fetch bytes
   * @param logicalIOConfiguration configuration for this logical IO implementation
   */
  public ParquetLogicalIOImpl(
      PhysicalIO physicalIO, LogicalIOConfiguration logicalIOConfiguration) {
    this.physicalIO = physicalIO;
    this.logicalIOConfiguration = logicalIOConfiguration;

    if (logicalIOConfiguration.isFooterCachingEnabled()) {
      new ParquetPrefetchTailTask(logicalIOConfiguration, physicalIO).get();
    }

    if (logicalIOConfiguration.isMetadataAwarePefetchingEnabled()
        && physicalIO.columnMappers() == null) {
      CompletableFuture.supplyAsync(new ParquetReadTailTask(logicalIOConfiguration, physicalIO))
          .thenAccept(
              (FileTail fileTail) ->
                  new ParquetMetadataTask(physicalIO, logicalIOConfiguration, fileTail).get());
    }
  }

  @Override
  public int read(long position) throws IOException {
    return physicalIO.read(position);
  }

  @Override
  public int read(byte[] buf, int off, int len, long position) throws IOException {
    if (logicalIOConfiguration.isMetadataAwarePefetchingEnabled()) {
      CompletableFuture.supplyAsync(
          new ParquetPrefetchRemainingColumnTask(
              physicalIO, logicalIOConfiguration, position, len));
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
}
