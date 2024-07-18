package com.amazon.connector.s3.io.logical.impl;

import com.amazon.connector.s3.io.logical.LogicalIO;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * A Parquet-aware implementation of a LogicalIO layer. It is capable of prefetching file tails,
 * parsing Parquet metadata and prefetching columns based on recent access patterns.
 */
public class ParquetLogicalIOImpl implements LogicalIO {

  private final S3URI s3Uri;

  // Configuration
  private final LogicalIOConfiguration logicalIOConfiguration;

  // Dependencies
  private final ParquetPrefetcher parquetPrefetcher;
  private final ParquetMetadataStore parquetMetadataStore;
  private final PhysicalIO physicalIO;

  /**
   * Constructs an instance of LogicalIOImpl.
   *
   * @param s3Uri s3Uri pointing to object to fetch
   * @param physicalIO underlying physical IO that knows how to fetch bytes
   * @param logicalIOConfiguration configuration for this logical IO implementation
   * @param parquetMetadataStore object where Parquet usage information is aggregated
   */
  public ParquetLogicalIOImpl(
      S3URI s3Uri,
      PhysicalIO physicalIO,
      LogicalIOConfiguration logicalIOConfiguration,
      ParquetMetadataStore parquetMetadataStore) {
    this.s3Uri = s3Uri;
    this.physicalIO = physicalIO;
    this.logicalIOConfiguration = logicalIOConfiguration;
    this.parquetMetadataStore = parquetMetadataStore;

    // Initialise prefetcher and start prefetching
    this.parquetPrefetcher =
        new ParquetPrefetcher(s3Uri, physicalIO, logicalIOConfiguration, parquetMetadataStore);
    this.parquetPrefetcher.prefetchFooterAndBuildMetadata();
  }

  @Override
  public int read(long position) throws IOException {
    return physicalIO.read(position);
  }

  @Override
  public int read(byte[] buf, int off, int len, long position) throws IOException {
    // Perform async prefetching before doing the blocking read
    this.parquetPrefetcher.prefetchRemainingColumnChunk(position, len);
    this.parquetPrefetcher.addToRecentColumnList(position);

    // Perform read
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
