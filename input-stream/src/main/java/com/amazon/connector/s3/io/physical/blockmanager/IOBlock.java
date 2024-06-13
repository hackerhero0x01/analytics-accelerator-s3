package com.amazon.connector.s3.io.physical.blockmanager;

import static com.amazon.connector.s3.util.Constants.ONE_KB;

import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.object.ObjectContent;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.NonNull;

class IOBlock implements Closeable {

  @Getter private final long start;
  @Getter private final long end;
  private CompletableFuture<ObjectContent> content;

  private final byte[] blockContent;

  private final int bufferSize;
  private static final int READ_BUFFER_SIZE = 64 * ONE_KB;

  public IOBlock(long start, long end, @NonNull CompletableFuture<ObjectContent> objectContent)
      throws IOException {
    Preconditions.checkState(start >= 0, "start must be non-negative");
    Preconditions.checkState(end >= 0, "end must be non-negative");
    Preconditions.checkState(start <= end, "start must not be bigger than end");

    this.start = start;
    this.end = end;
    this.content = objectContent;
    this.bufferSize = (int) size();
    this.blockContent = new byte[(int) size()];

    readIntoBuffer();
  }

  public int getByte(long pos) {
    return Byte.toUnsignedInt(this.blockContent[positionToOffset(pos)]);
  }

  public int remainingInBuffer(long pos) {
    return bufferSize - positionToOffset(pos);
  }

  public void read(byte[] buff, int len, long pos) {
    for (int i = 0; i < len; i++) {
      buff[i] = blockContent[positionToOffset(pos) + i];
    }
  }

  private void readIntoBuffer() throws IOException {

    int numBytesToRead;
    int numBytesRemaining = this.bufferSize;
    int bytesRead;
    int posInBuffer = 0;
    byte[] buffer = new byte[READ_BUFFER_SIZE];

    try (InputStream inputStream = this.content.join().getStream()) {
      while (numBytesRemaining > 0) {
        numBytesToRead = Math.min(READ_BUFFER_SIZE, numBytesRemaining);
        bytesRead = inputStream.read(buffer, 0, numBytesToRead);

        if (bytesRead < 0) {
          String message =
              String.format("Unexpected end of stream: numRemainingBytes = %d", numBytesRemaining);
          throw new EOFException(message);
        }

        if (bytesRead > 0) {
          numBytesRemaining -= bytesRead;
          for (int i = 0; i < bytesRead; i++) {
            blockContent[posInBuffer++] = buffer[i];
          }
        }

        if (bytesRead == 0) {
          return;
        }
      }
    }
  }

  public boolean contains(long pos) {
    return start <= pos && pos <= end;
  }

  public long size() {
    return end - start + 1;
  }

  /**
   * A mapping between object byte locations and byte buffer byte locations.
   *
   * @param pos the position in the object
   * @return off the offset corresponding to `pos`
   */
  private int positionToOffset(long pos) {
    return (int) (pos - start);
  }

  @Override
  public void close() throws IOException {
    this.content.join().getStream().close();
  }
}
