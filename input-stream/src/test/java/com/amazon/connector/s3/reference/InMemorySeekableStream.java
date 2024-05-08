package com.amazon.connector.s3.reference;

import com.amazon.connector.s3.SeekableInputStream;
import java.nio.ByteBuffer;
import org.junit.platform.commons.util.Preconditions;

public class InMemorySeekableStream extends SeekableInputStream {

  private ByteBuffer data;
  private long position;
  private int contentLength;

  /**
   * Creates an in memory seekable stream backed by a byte buffer.
   *
   * @param data the underlying byte buffer where data will be fetched from
   */
  public InMemorySeekableStream(byte[] data) {
    this.data = ByteBuffer.wrap(data);
    this.contentLength = data.length;
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
  public int readTail(byte[] buf, int off, int n) {
    // Save position of stream
    long prevPosition = this.position;

    long tailStart = contentLength - n;
    data.position((int) tailStart);
    data.get(buf, off, n);

    // Reset position
    this.position = prevPosition;
    data.position((int) this.position);

    return n;
  }

  @Override
  public int read() {
    if (this.position >= this.contentLength) {
      return -1;
    }
    data.position((int) this.position);
    this.position++;

    return Byte.toUnsignedInt(this.data.get());
  }

  @Override
  public int read(byte[] buffer, int offset, int len) {
    if (this.position >= this.contentLength) {
      return -1;
    }

    data.position((int) this.position);

    // Only ever ask till end of byte buffer to prevent BufferUnderFlowExceptions.
    if (this.position + len >= this.contentLength) {
      len = (int) (this.contentLength - this.position);
    }

    data.get(buffer, offset, len);
    this.position += len;

    return len;
  }
}
