package com.amazon.connector.s3;

import com.amazon.connector.s3.blockmanager.BlockManager;
import com.google.common.base.Preconditions;
import java.io.IOException;

/**
 * High throughput seekable stream used to read data from Amazon S3.
 *
 * <p>Don't share between threads. The current implementation is not thread safe in that calling
 * {@link #seek(long) seek} will modify the position of the stream and the behaviour of calling
 * {@link #seek(long) seek} and {@link #read() read} concurrently from two different threads is
 * undefined.
 */
public class S3SeekableInputStream extends SeekableInputStream {

  private final BlockManager blockManager;
  private long position;

  /**
   * Creates a new instance of {@link S3SeekableInputStream}.
   *
   * @param blockManager an instance of {@link BlockManager}.
   */
  public S3SeekableInputStream(BlockManager blockManager) throws IOException {
    Preconditions.checkNotNull(blockManager, "BlockManager must not be null");

    this.blockManager = blockManager;
    this.position = 0;
  }

  @Override
  public int read() throws IOException {
    if (this.position >= contentLength()) {
      return -1;
    }

    int byteRead = this.blockManager.readByte(this.position);
    this.position++;
    return byteRead;
  }

  @Override
  public void seek(long pos) {
    Preconditions.checkState(pos >= 0, "position must be non-negative");
    Preconditions.checkState(
        pos < contentLength(), "zero-indexed position must be less than the object size");

    this.position = pos;
  }

  @Override
  public long getPos() {
    return this.position;
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.blockManager.close();
  }

  private long contentLength() {
    return this.blockManager.getMetadata().join().getContentLength();
  }
}
