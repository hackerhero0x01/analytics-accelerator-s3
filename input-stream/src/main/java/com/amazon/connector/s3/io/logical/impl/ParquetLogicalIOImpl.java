package com.amazon.connector.s3.io.logical.impl;

import com.amazon.connector.s3.io.logical.LogicalIO;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.object.ObjectMetadata;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * A basic proxying implementation of a LogicalIO layer. To be extended later with logical
 * optimisations (for example, reading Parquet footers and interpreting Parquet metadata).
 */
public class ParquetLogicalIOImpl implements LogicalIO {

  private final PhysicalIO physicalIO;

  /**
   * Constructs an instance of LogicalIOImpl.
   *
   * @param physicalIO underlying physical IO that knows how to fetch bytes
   */
  public ParquetLogicalIOImpl(PhysicalIO physicalIO) {
    this.physicalIO = physicalIO;
  }

  @Override
  public int read(long position) throws IOException {
    return physicalIO.read(position);
  }

  @Override
  public int read(byte[] buf, int off, int len, long position) throws IOException {
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
