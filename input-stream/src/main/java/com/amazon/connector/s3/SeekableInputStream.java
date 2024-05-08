package com.amazon.connector.s3;

import java.io.IOException;
import java.io.InputStream;

/**
 * A SeekableInputStream is like a conventional InputStream but equipped with two additional
 * operations: {@link #seek(long) seek} and {@link #getPos() getPos}. Typically, seekable streams
 * are used for random data access (i.e, data access that is not strictly sequential or requires
 * backwards seeks).
 *
 * <p>Implementations should implement {@link #close() close} to release resources.
 */
public abstract class SeekableInputStream extends InputStream {

  /**
   * Seeks (jumps) to a position inside the stream.
   *
   * @param pos The position to jump to in the stream given in bytes (zero-indexed).
   * @throws IOException
   */
  public abstract void seek(long pos) throws IOException;

  /**
   * Returns the current position in the stream.
   *
   * @return the position in the stream
   */
  public abstract long getPos();

  /**
   * Reads the last n bytes from the stream into a byte buffer. Blocks until end of stream is
   * reached. Leaves the position of the stream unaltered.
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param n the number of bytes to read; the n-th byte should be the last byte of the stream.
   * @return the total number of bytes read into the buffer
   */
  public abstract int readTail(byte[] buf, int off, int n);
}
