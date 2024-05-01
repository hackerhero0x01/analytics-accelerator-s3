package com.amazon.connector.s3.blockmanager;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.NonNull;
import software.amazon.awssdk.core.async.ResponsePublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

class IOBlock implements Closeable {
  private final long start;
  private final long end;
  private final AtomicLong limit;

  private CompletableFuture<ResponsePublisher<GetObjectResponse>> content;
  private final byte[] blockContent;

  public IOBlock(long start, long end, @NonNull CompletableFuture<ResponsePublisher<GetObjectResponse>> objectContent) {
    Preconditions.checkState(start >= 0, "start must be non-negative");
    Preconditions.checkState(end >= 0, "end must be non-negative");
    Preconditions.checkState(start <= end, "start must not be bigger than end");

    this.start = start;
    this.end = end;
    this.limit = new AtomicLong(start);
    this.content = objectContent;

    this.blockContent = new byte[(int) size()];

    // Ask for the whole range and keep filling the buffer as bytes become available
    this.content.thenAccept(responsePublisher -> {
      responsePublisher.subscribe(byteBuffer -> {
        byte[] b = byteBuffer.array();

        long oldLimit = this.limit.get();
        long nextByteToFill = oldLimit;
        for (int i = 0; i < b.length; ++i, ++nextByteToFill) {
          this.blockContent[positionToOffset(nextByteToFill)] = b[i];
        }
        this.limit.compareAndSet(oldLimit, nextByteToFill);
      });
    });
  }

  public int read(long start, byte[] buf, int off, int len) {
    int available = (int) (this.limit.get() - start);

    int bytesRead = 0;
    for (int i = off; bytesRead < Math.min(available, len); ++i, bytesRead++) {
      buf[i] = this.blockContent[positionToOffset(start + i)];
    }
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
}
