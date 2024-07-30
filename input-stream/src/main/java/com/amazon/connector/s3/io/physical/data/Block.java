package com.amazon.connector.s3.io.physical.data;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.request.ReadMode;
import com.amazon.connector.s3.request.Referrer;
import com.amazon.connector.s3.util.S3URI;
import com.amazon.connector.s3.util.StreamUtils;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;

/**
 * A Block holding part of an object's data and owning its own async process for fetching part of
 * the object.
 */
public class Block implements Closeable {

  private CompletableFuture<ObjectContent> source;
  private CompletableFuture<byte[]> data;

  @Getter private final long start;
  @Getter private final long end;
  @Getter private final long generation;
  @Getter private final PhysicalIOConfiguration configuration;

  /**
   * Constructs a Block.
   *
   * @param s3URI the S3 URI of the object
   * @param objectClient the object client to use to interact with the object store
   * @param start start of the block
   * @param end end of the block
   * @param generation generation of the block in a sequential read pattern (should be 0 by default)
   * @param readMode read mode describing whether this is a sync or async fetch
   * @param configuration the PhysicalIO configuration
   */
  public Block(
      S3URI s3URI,
      ObjectClient objectClient,
      long start,
      long end,
      long generation,
      ReadMode readMode,
      PhysicalIOConfiguration configuration) {
    Preconditions.checkNotNull(s3URI, "`s3URI` should not be null");
    Preconditions.checkNotNull(objectClient, "`objectClient` should not be null");
    Preconditions.checkNotNull(readMode, "`readMode` should not be null");
    Preconditions.checkNotNull(configuration, "`configuration` should not be null");

    Preconditions.checkArgument(
        0 <= generation, "`generation` must be non-negative; was: %s", generation);
    Preconditions.checkArgument(0 <= start, "`start` must be non-negative; was: %s", start);
    Preconditions.checkArgument(0 <= end, "`end` must be non-negative; was: %s", end);
    Preconditions.checkArgument(
        start <= end, "`start` must be less than `end`; %s is not less than %s", start, end);

    this.start = start;
    this.end = end;
    this.generation = generation;
    this.configuration = configuration;

    Range range = new Range(start, end);
    this.source =
        objectClient.getObject(
            GetRequest.builder()
                .bucket(s3URI.getBucket())
                .key(s3URI.getKey())
                .range(range)
                .referrer(new Referrer(range.toString(), readMode))
                .build());

    this.data = this.source.thenApply(StreamUtils::toByteArray);
  }

  /**
   * Reads a byte from the underlying object
   *
   * @param pos The position to read
   * @return an unsigned int representing the byte that was read
   */
  public int read(long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    byte[] content = getContent();
    return Byte.toUnsignedInt(content[posToOffset(pos)]);
  }

  /**
   * Reads data into the provided buffer
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @return the total number of bytes read into the buffer
   */
  public int read(byte[] buf, int off, int len, long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(0 <= off, "`off` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    Preconditions.checkArgument(off < buf.length, "`off` must be less than size of buffer");

    byte[] content = getContent();
    int available = content.length - posToOffset(pos);
    int bytesToCopy = Math.min(len, available);

    for (int i = 0; i < bytesToCopy; ++i) {
      buf[off + i] = content[posToOffset(pos) + i];
    }

    return bytesToCopy;
  }

  /**
   * Does this block contain the position?
   *
   * @param pos the position
   * @return true if the byte at the position is contained by this block
   */
  public boolean contains(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    return start <= pos && pos <= end;
  }

  /**
   * Determines the offset in the Block corresponding to a position in an object.
   *
   * @param pos the position of a byte in the object
   * @return the offset in the byte buffer underlying this Block
   */
  private int posToOffset(long pos) {
    return (int) (pos - start);
  }

  private byte[] getContent() throws IOException {
    try {
      return this.data.get(configuration.getRequestTimeoutMillis(), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new IOException("Exception when retrieving data from the object store", e);
    }
  }

  @Override
  public void close() {
    this.source.cancel(false);
    this.data.cancel(false);

    this.source = null;
    this.data = null;
  }
}
