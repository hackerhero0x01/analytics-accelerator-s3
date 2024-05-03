package com.amazon.connector.s3.blockmanager;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.NonNull;

/**
 * A block manager in charge of fetching bytes from an object store. Currently: - Block Manager
 * fetches bytes in 8MB chunks by default - IO blocks are fixed in size (at most 8MB) and do not
 * grow beyond their original size - Block Manager keeps the last 10 blocks alive in memory --
 * technically speaking this is caching, but we should be able to naturally extend this logic into
 * prefetching. - If an 11th chunk is requested, then the oldest chunk is released along with all
 * the resources it is holding.
 */
public class BlockManager implements AutoCloseable {
  private static final int MAX_BLOCK_COUNT = 10;
  private static final long EIGHT_MB_IN_BYTES = 8 * 1024 * 1024;
  private static final long DEFAULT_BLOCK_SIZE = EIGHT_MB_IN_BYTES;

  @Getter private final CompletableFuture<ObjectMetadata> metadata;
  private final AutoClosingCircularBuffer<IOBlock> ioBlocks;

  private final ObjectClient objectClient;
  private final S3URI s3URI;
  private long blockSize = DEFAULT_BLOCK_SIZE;

  /**
   * Creates an instance of block manager.
   *
   * @param objectClient the Object Client to use to fetch the data
   * @param blockSize size of block to use, defaults to 8MB
   * @param s3URI the location of the object
   */
  public BlockManager(@NonNull ObjectClient objectClient, @NonNull S3URI s3URI, long blockSize) {
    this.objectClient = objectClient;
    this.s3URI = s3URI;
    this.metadata =
        objectClient.headObject(
            HeadRequest.builder().bucket(s3URI.getBucket()).key(s3URI.getKey()).build());

    if (blockSize > 0) {
      this.blockSize = blockSize;
    }

    this.ioBlocks = new AutoClosingCircularBuffer<>(MAX_BLOCK_COUNT);
  }

  /**
   * Reads a byte from the underlying object
   *
   * @param pos The position to read
   * @return an unsigned int representing the byte that was read
   */
  public int readByte(long pos) {
    return getBlockForPosition(pos).getByte(pos);
  }

  /**
   * Reads request data ito the provided buffer
   *
   * @param buffer buffer to read data into
   * @param offset start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @return the total number of bytes read into the buffer
   */
  public int readIntoBuffer(byte[] buffer, int offset, int len, long pos) {

    int numBytesRead = 0;
    int numBytesRemaining = len;
    long nextReadPos = pos;
    int nextReadOffset = offset;

    while (numBytesRemaining > 0) {

      // Reached EOF
      if (nextReadPos > getLastObjectByte()) {
        return numBytesRead;
      }

      IOBlock ioBlock = getBlockForPosition(nextReadPos);

      ioBlock.setPositionInBuffer(nextReadPos);
      ByteBuffer blockData = ioBlock.getBlockContent();

      int numBytesToRead = Math.min(blockData.remaining(), numBytesRemaining);
      blockData.get(buffer, nextReadOffset, numBytesToRead);
      nextReadOffset += numBytesToRead;
      nextReadPos += numBytesToRead;
      numBytesRemaining -= numBytesToRead;
      numBytesRead += numBytesToRead;
    }

    return numBytesRead;
  }

  private IOBlock getBlockForPosition(long pos) {
    return lookupBlockForPosition(pos)
        .orElseGet(
            () -> {
              try {
                return createBlockStartingAt(pos);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
  }

  private Optional<IOBlock> lookupBlockForPosition(long pos) {
    return ioBlocks.stream().filter(ioBlock -> ioBlock.contains(pos)).findFirst();
  }

  private IOBlock createBlockStartingAt(long start) throws IOException {
    long end = Math.min(start + blockSize - 1, getLastObjectByte());

    CompletableFuture<ObjectContent> objectContent =
        this.objectClient.getObject(
            GetRequest.builder()
                .bucket(s3URI.getBucket())
                .key(s3URI.getKey())
                .range(Range.builder().start(start).end(end).build())
                .build());

    IOBlock ioBlock = new IOBlock(start, end, objectContent);
    ioBlocks.add(ioBlock);
    return ioBlock;
  }

  private long getLastObjectByte() {
    return this.metadata.join().getContentLength() - 1;
  }

  @Override
  public void close() throws IOException {
    this.ioBlocks.close();
    this.objectClient.close();
  }
}
