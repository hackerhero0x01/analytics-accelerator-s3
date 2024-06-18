package com.amazon.connector.s3.io.physical.blockmanager;

import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.object.ObjectMetadata;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** A block manager interface for a single object. */
public interface BlockManagerInterface extends AutoCloseable {
  /**
   * Reads a byte from the underlying object
   *
   * @param pos The position to read
   * @return an unsigned int representing the byte that was read
   */
  int read(long pos) throws IOException;

  /**
   * Reads request data into the provided buffer
   *
   * @param buffer buffer to read data into
   * @param offset start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @return the total number of bytes read into the buffer
   */
  int read(byte[] buffer, int offset, int len, long pos) throws IOException;

  /**
   * Reads the last n bytes from the object.
   *
   * @param buf byte buffer to read into
   * @param off position of first read byte in the byte buffer
   * @param n length of data to read in bytes
   * @return the number of bytes read or -1 when EOF is reached
   */
  int readTail(byte[] buf, int off, int n) throws IOException;

  /**
   * Queue a prefetch request for the given ranges. The request will be processed asynchronously.
   *
   * @param prefetchRanges the ranges to prefetch
   */
  void queuePrefetch(List<Range> prefetchRanges);

  /**
   * Get the metadata of the object
   *
   * @return the metadata of the object
   */
  CompletableFuture<ObjectMetadata> getMetadata();

  ColumnMappers getColumnMappers();

  void putColumnMappers(ColumnMappers columnMappers);
}
