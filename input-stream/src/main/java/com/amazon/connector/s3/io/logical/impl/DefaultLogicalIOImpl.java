package com.amazon.connector.s3.io.logical.impl;

import com.amazon.connector.s3.io.logical.LogicalIO;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.request.ObjectMetadata;
import java.io.IOException;
import lombok.NonNull;

/** The default implementation of a LogicalIO layer. Will be used for all non-parquet files. */
public class DefaultLogicalIOImpl implements LogicalIO {

  // Dependencies
  private final PhysicalIO physicalIO;

  /**
   * Constructs an instance of LogicalIOImpl.
   *
   * @param physicalIO underlying physical IO that knows how to fetch bytes
   */
  public DefaultLogicalIOImpl(@NonNull PhysicalIO physicalIO) {
    this.physicalIO = physicalIO;
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
   * Reads data into the provided buffer
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
    // Perform read
    return physicalIO.read(buf, off, len, position);
  }

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
   * Closes associate resources.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    physicalIO.close();
  }
}
