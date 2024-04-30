package com.amazon.connector.s3.blockmanager;

import com.amazon.connector.s3.object.ObjectContent;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

class IOBlock implements Closeable {
  private final long start;
  private final long end;
  private long positionInCurrentBuffer;
  private CompletableFuture<ObjectContent> content;
  private final byte[] blockContent;

  public IOBlock(long start, long end, @NonNull CompletableFuture<ObjectContent> objectContent) {
    Preconditions.checkState(start >= 0, "start must be non-negative");
    Preconditions.checkState(end >= 0, "end must be non-negative");
    Preconditions.checkState(start <= end, "start must not be bigger than end");

    this.start = start;
    this.end = end;
    this.positionInCurrentBuffer = start;
    this.content = objectContent;

    this.blockContent = new byte[(int) size()];
  }

  public int getByte(long pos) throws IOException {
    if (pos < positionInCurrentBuffer) {
      return Byte.toUnsignedInt(this.blockContent[positionToOffset(pos)]);
    }

    return readByte(pos);
  }

  private int readByte(long pos) throws IOException {
    Preconditions.checkState(
        positionInCurrentBuffer <= pos,
        String.format(
            "byte at position %s was fetched already and should have been served via 'getByte'",
            pos));
    Preconditions.checkState(pos <= end, "pos must be less than end");

    for (; positionInCurrentBuffer <= pos; ++positionInCurrentBuffer) {
      int byteRead = this.content.join().getStream().read();

      if (byteRead < 0) {
        throw new IOException(
            String.format(
                "Premature end of file. Did not expect to read -1 at position %s",
                positionInCurrentBuffer));
      }

      this.blockContent[positionToOffset(positionInCurrentBuffer)] = (byte) byteRead;
    }

    return Byte.toUnsignedInt(this.blockContent[positionToOffset(pos)]);
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
    this.content.join().getStream().close();
  }
}
