package com.amazon.connector.s3;

import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.logical.LogicalIO;
import com.amazon.connector.s3.io.logical.impl.ParquetLogicalIOImpl;
import com.amazon.connector.s3.io.logical.impl.ParquetMetadataStore;
import com.amazon.connector.s3.io.physical.data.BlobStore;
import com.amazon.connector.s3.io.physical.data.MetadataStore;
import com.amazon.connector.s3.io.physical.impl.PhysicalIOImpl;
import com.amazon.connector.s3.util.S3URI;
import java.io.EOFException;
import java.io.IOException;
import lombok.NonNull;

/**
 * High throughput seekable stream used to read data from Amazon S3.
 *
 * <p>Don't share between threads. The current implementation is not thread safe in that calling
 * {@link #seek(long) seek} will modify the position of the stream and the behaviour of calling
 * {@link #seek(long) seek} and {@link #read() read} concurrently from two different threads is
 * undefined.
 */
public class S3SeekableInputStream extends SeekableInputStream {
  private final LogicalIO logicalIO;
  private long position;

  /**
   * Creates a new instance of {@link S3SeekableInputStream}. This version of the constructor
   * initialises the stream with sensible defaults.
   *
   * @param s3URI the object this stream is using
   * @param metadataStore a MetadataStore instance being used as a HeadObject cache
   * @param blobStore a BlobStore instance being used as data cache
   * @param telemetry The {@link Telemetry} to use to report measurements.
   * @param configuration provides instance of {@link S3SeekableInputStreamConfiguration}
   * @param parquetMetadataStore object containing Parquet usage information
   */
  S3SeekableInputStream(
      @NonNull S3URI s3URI,
      @NonNull MetadataStore metadataStore,
      @NonNull BlobStore blobStore,
      @NonNull Telemetry telemetry,
      @NonNull S3SeekableInputStreamConfiguration configuration,
      @NonNull ParquetMetadataStore parquetMetadataStore) {
    this(
        new ParquetLogicalIOImpl(
            s3URI,
            new PhysicalIOImpl(s3URI, metadataStore, blobStore, telemetry),
            telemetry,
            configuration.getLogicalIOConfiguration(),
            parquetMetadataStore),
        telemetry);
  }

  /**
   * Given a LogicalIO, creates a new instance of {@link S3SeekableInputStream}. This version of the
   * constructor is useful for testing as it allows dependency injection.
   *
   * @param logicalIO already initialised LogicalIO
   * @param telemetry The {@link Telemetry} to use to report measurements.
   */
  S3SeekableInputStream(@NonNull LogicalIO logicalIO, @NonNull Telemetry telemetry) {
    this.logicalIO = logicalIO;
    this.position = 0;
  }

  @Override
  public int read() throws IOException {
    if (this.position >= contentLength()) {
      return -1;
    }

    int byteRead = this.logicalIO.read(this.position);
    this.position++;
    return byteRead;
  }

  @Override
  public int read(byte[] buffer, int offset, int len) throws IOException {
    if (this.position >= contentLength()) {
      return -1;
    }

    int numBytesRead = this.logicalIO.read(buffer, offset, len, position);

    if (numBytesRead < 0) {
      return numBytesRead;
    }

    this.position += numBytesRead;
    return numBytesRead;
  }

  @Override
  public void seek(long pos) throws IOException {
    // TODO: https://app.asana.com/0/1206885953994785/1207207312934251/f
    // S3A throws an EOFException here, S3FileIO does IllegalArgumentException
    Preconditions.checkArgument(pos >= 0, "position must be non-negative");

    if (pos >= contentLength()) {
      throw new EOFException("zero-indexed seek position must be less than the object size");
    }

    this.position = pos;
  }

  @Override
  public long getPos() {
    return this.position;
  }

  @Override
  public int readTail(byte[] buf, int off, int n) throws IOException {
    return logicalIO.readTail(buf, off, n);
  }

  @Override
  public void close() throws IOException {
    this.logicalIO.close();
  }

  private long contentLength() {
    return this.logicalIO.metadata().getContentLength();
  }
}
