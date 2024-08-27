package com.amazon.connector.s3.io.logical.impl;

import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.logical.LogicalIO;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.request.ObjectMetadata;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import lombok.NonNull;

/**
 * A Parquet-aware implementation of a LogicalIO layer. It is capable of prefetching file tails,
 * parsing Parquet metadata and prefetching columns based on recent access patterns.
 */
public class ParquetLogicalIOImpl implements LogicalIO {
  // Dependencies
  private final ParquetPrefetcher parquetPrefetcher;
  private final PhysicalIO physicalIO;

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
    this.physicalIO = physicalIO;

    // Initialise prefetcher and start prefetching
    this.parquetPrefetcher =
        new ParquetPrefetcher(
            s3Uri, physicalIO, telemetry, logicalIOConfiguration, parquetMetadataStore);
    this.parquetPrefetcher.prefetchFooterAndBuildMetadata();
  }

  /**
   * Reads a byte from the given position.
   *
   * @param position the position to read
   * @return an unsigned int representing the byte that was read
   * @throws IOException IO error, if incurred.
   */
  @Override
  public int read(long position) throws IOException {
    return physicalIO.read(position);
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

    // Perform read
    return physicalIO.read(buf, off, len, position);
  }

  /**
   * Reads the last n bytes from the stream into a byte buffer. Blocks until end of stream is
   * reached. Leaves the position of the stream unaltered.
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len the number of bytes to read; the n-th byte should be the last byte of the stream.
   * @return the total number of bytes read into the buffer
   */
  @Override
  public int readTail(byte[] buf, int off, int len) throws IOException {
    return physicalIO.readTail(buf, off, len);
  }

  /**
   * Returns object metadata.
   *
   * @return object metadata
   */
  @Override
  public ObjectMetadata metadata() {
    return this.physicalIO.metadata();
  }

  /**
   * Closes the resources associated with the {@link ParquetLogicalIOImpl}.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    physicalIO.close();
  }
}
