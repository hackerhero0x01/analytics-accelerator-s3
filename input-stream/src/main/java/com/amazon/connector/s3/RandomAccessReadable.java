package com.amazon.connector.s3;

import com.amazon.connector.s3.object.ObjectMetadata;
import java.io.Closeable;
import java.io.IOException;

/** An InputStream-like entity implementing blocking random-access reads. */
public interface RandomAccessReadable extends Closeable {
  /**
   * Returns object metadata.
   *
   * @return the metadata of the object.
   */
  ObjectMetadata metadata();

  /**
   * Reads a byte from the underlying object
   *
   * @param pos the position to read
   * @return an unsigned int representing the byte that was read
   */
  int read(long pos) throws IOException;

  /**
   * Reads request data into the provided buffer
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @return the total number of bytes read into the buffer
   */
  int read(byte[] buf, int off, int len, long pos) throws IOException;

  /**
   * Reads the last n bytes from the stream into a byte buffer. Blocks until end of stream is
   * reached. Leaves the position of the stream unaltered.
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len the number of bytes to read; the n-th byte should be the last byte of the stream.
   * @return the total number of bytes read into the buffer
   */
  int readTail(byte[] buf, int off, int len) throws IOException;
}
