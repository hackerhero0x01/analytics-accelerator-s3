package com.amazon.connector.s3.reference;

import com.amazon.connector.s3.SeekableInputStream;
import org.junit.platform.commons.util.Preconditions;

public class InMemorySeekableStream extends SeekableInputStream {

  private byte[] data;
  private long position;

  /**
   * Creates an in memory seekable stream backed by a byte buffer.
   *
   * @param data the underlying byte buffer where data will be fetched from
   */
  public InMemorySeekableStream(byte[] data) {
    this.data = data;
  }

  @Override
  public void seek(long pos) {
    Preconditions.condition(pos >= 0, "Position should be non-negative");
    this.position = pos;
  }

  @Override
  public long getPos() {
    return this.position;
  }

  @Override
  public int read() {
    if (this.position >= this.data.length) {
      return -1;
    }

    return Byte.toUnsignedInt(data[(int) position++]);
  }
}
