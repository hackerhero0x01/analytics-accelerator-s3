package com.amazon.connector.s3.blockmanager;

import com.amazon.connector.s3.object.ObjectContent2;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.NonNull;

class IOBlock implements Closeable {
  private final long start;
  @Getter private final long end;
  private long numBytesRead = 0;

  // is a prediction token still available?
  private boolean prefetchToken = true;
  private final AtomicLong limit;

  private CompletableFuture<ObjectContent2> content;
  private final byte[] blockContent;

  public IOBlock(long start, long end, @NonNull CompletableFuture<ObjectContent2> objectContent) {
    Preconditions.checkState(start >= 0, "start must be non-negative");
    Preconditions.checkState(end >= 0, "end must be non-negative");
    Preconditions.checkState(start <= end, "start must not be bigger than end");

    this.start = start;
    this.end = end;
    this.limit = new AtomicLong(start); // points to the first byte NOT YET fetched
    this.content = objectContent;

    this.blockContent = new byte[(int) size()];

    // Ask for the whole range and keep filling the buffer as bytes become available
    this.content.thenAccept(
        objectContent2 -> {
          objectContent2
              .getPublisher()
              .subscribe(
                  byteBuffer -> {
                    long oldLimit = this.limit.get();
                    long nextByteToFill = oldLimit;

                    while (byteBuffer.remaining() > 0) {
                      this.blockContent[positionToOffset(nextByteToFill)] = byteBuffer.get();
                      ++nextByteToFill;
                    }

                    this.limit.compareAndSet(oldLimit, nextByteToFill);
                  });
        });
  }

  public int read(long start, byte[] buf, int off, int len) {
    int available = (int) (this.limit.get() - start);
    while (available == 0) {
      available = (int) (this.limit.get() - start);
    }

    int bytesRead = 0;
    for (; bytesRead < Math.min(available, len); bytesRead++) {
      buf[off + bytesRead] = this.blockContent[positionToOffset(start + bytesRead)];
    }

    numBytesRead += bytesRead;
    return bytesRead;
  }

  public boolean contains(long pos) {
    return start <= pos && pos <= end;
  }

  public long size() {
    return end - start + 1;
  }

  /** A mapping between object byte locations and byte buffer byte locations */
  private int positionToOffset(long pos) {
    return (int) (pos - start);
  }

  @Override
  public void close() throws IOException {
    // is this a noop?
  }

  public int getUtilisation() {
    return (int) (100 * numBytesRead / size());
  }

  public boolean shouldPrefetch() {
    return prefetchToken && getUtilisation() > 50;
  }

  public void takePrefetchToken() {
    this.prefetchToken = false;
  }
}
