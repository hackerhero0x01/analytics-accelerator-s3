package com.amazon.connector.s3;

import java.io.IOException;
import java.io.InputStream;

/**
 * A SeekableInputStream is like a conventional InputStream but equipped with two additional
 * operations: 'seek' and 'getPos'. Typically, seekable streams are used for random data access
 * (i.e, data access that is not strictly sequential or requires backwards seeks).
 *
 * Implementations should implement 'close' to release resources.
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
}
