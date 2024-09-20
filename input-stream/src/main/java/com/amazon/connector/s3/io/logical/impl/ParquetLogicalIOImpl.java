package com.amazon.connector.s3.io.logical.impl;

import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import lombok.NonNull;

/**
 * A Parquet-aware implementation of a LogicalIO layer. It is capable of prefetching file tails,
 * parsing Parquet metadata and prefetching columns based on recent access patterns.
 */
public class ParquetLogicalIOImpl extends DefaultLogicalIOImpl {
  // Dependencies
  private final ParquetPrefetcher parquetPrefetcher;

  /**
   * Constructs an instance of LogicalIOImpl.
   *
   * @param s3Uri s3Uri pointing to object to fetch
   * @param physicalIO underlying physical IO that knows how to fetch bytes
   * @param telemetry an instance of {@link Telemetry} to use
   * @param logicalIOConfiguration configuration for this logical IO implementation
   * @param parquetMetadataStore object where Parquet usage information is aggregated
   */
  public ParquetLogicalIOImpl(
      @NonNull S3URI s3Uri,
      @NonNull PhysicalIO physicalIO,
      @NonNull Telemetry telemetry,
      @NonNull LogicalIOConfiguration logicalIOConfiguration,
      @NonNull ParquetMetadataStore parquetMetadataStore) {
    super(physicalIO);

    // Initialise prefetcher and start prefetching
    this.parquetPrefetcher =
        new ParquetPrefetcher(
            s3Uri, physicalIO, telemetry, logicalIOConfiguration, parquetMetadataStore);
    this.parquetPrefetcher.prefetchFooterAndBuildMetadata();
  }

  /**
   * Reads Reads data into the provided buffer
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param position the position to begin reading from
   * @return an unsigned int representing the byte that was read
   * @throws IOException IO error, if incurred.
   */
  @Override
  public int read(byte[] buf, int off, int len, long position) throws IOException {
    // Perform async prefetching before doing the blocking read
    this.parquetPrefetcher.prefetchRemainingColumnChunk(position, len);
    this.parquetPrefetcher.addToRecentColumnList(position);

    return super.read(buf, off, len, position);
  }
}
