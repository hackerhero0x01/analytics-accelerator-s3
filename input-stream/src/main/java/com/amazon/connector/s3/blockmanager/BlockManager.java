package com.amazon.connector.s3.blockmanager;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.core.async.ResponsePublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * A block manager in charge of fetching bytes from an object store. Currently: - Block Manager
 * fetches bytes in 8MB chunks - IO blocks are fixed in size (at most 8MB) and do not grow beyond
 * their original size - Block Manager keeps the last 10 blocks alive in memory -- technically
 * speaking this is caching, but we should be able to naturally extend this logic into prefetching.
 * - If an 11th chunk is requested, then the oldest chunk is released along with all the resources
 * it is holding.
 */
public class BlockManager implements AutoCloseable {
  private static final int MAX_BLOCK_COUNT = 10;
  private static final long EIGHT_MB_IN_BYTES = 8 * 1024 * 1024;
  private static final long DEFAULT_BLOCK_SIZE = EIGHT_MB_IN_BYTES;

  @Getter private final CompletableFuture<ObjectMetadata> metadata;
  private final AutoClosingCircularBuffer<IOBlock> ioBlocks;

  private final ObjectClient objectClient;
  private final S3URI s3URI;

  /**
   * Creates an instance of block manager.
   *
   * @param objectClient the Object Client to use to fetch the data
   * @param s3URI the location of the object
   */
  public BlockManager(@NonNull ObjectClient objectClient, @NonNull S3URI s3URI) {
    this.objectClient = objectClient;
    this.s3URI = s3URI;
    this.metadata =
        objectClient.headObject(
            HeadRequest.builder().bucket(s3URI.getBucket()).key(s3URI.getKey()).build());

    this.ioBlocks = new AutoClosingCircularBuffer<>(MAX_BLOCK_COUNT);
  }

  public int read(long pos, byte[] buf, int off, int len) {
    return getBlockForPosition(pos).read(pos, buf, off, len);
  }

  private IOBlock getBlockForPosition(long pos) {
    return lookupBlockForPosition(pos).orElseGet(() -> createBlockStartingAt(pos));
  }

  private Optional<IOBlock> lookupBlockForPosition(long pos) {
    return ioBlocks.stream().filter(ioBlock -> ioBlock.contains(pos)).findFirst();
  }

  private IOBlock createBlockStartingAt(long start) {
    long end = Math.min(start + DEFAULT_BLOCK_SIZE, getLastObjectByte());

    CompletableFuture<ResponsePublisher<GetObjectResponse>> objectContent =
        this.objectClient.getObject2(
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
