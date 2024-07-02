package com.amazon.connector.s3.io.physical.blockmanager;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.request.Referrer;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A multi objects block manager in charge of fetching bytes from an object store for multiple
 * objects.
 */
public class MultiObjectsBlockManager implements AutoCloseable {

  private final Map<S3URI, CompletableFuture<ObjectMetadata>> metadata;
  private final Map<S3URI, AutoClosingCircularBuffer<IOBlock>> ioBlocks;
  private final Map<S3URI, AutoClosingCircularBuffer<PrefetchIOBlock>> prefetchCache;

  private final ObjectClient objectClient;
  private final BlockManagerConfiguration configuration;

  private static final Logger LOG = LogManager.getLogger(MultiObjectsBlockManager.class);

  /**
   * Creates an instance of block manager.
   *
   * @param objectClient the Object Client to use to fetch the data
   * @param configuration configuration
   */
  public MultiObjectsBlockManager(
      @NonNull ObjectClient objectClient, @NonNull BlockManagerConfiguration configuration) {
    this(
        objectClient,
        configuration,
        Collections.synchronizedMap(
            new LinkedHashMap<S3URI, CompletableFuture<ObjectMetadata>>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry eldest) {
                return this.size() > configuration.getCapacityMultiObjects();
              }
            }),
        Collections.synchronizedMap(
            new LinkedHashMap<S3URI, AutoClosingCircularBuffer<IOBlock>>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry eldest) {
                return this.size() > configuration.getCapacityMultiObjects();
              }
            }),
        Collections.synchronizedMap(
            new LinkedHashMap<S3URI, AutoClosingCircularBuffer<PrefetchIOBlock>>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry eldest) {
                return this.size() > configuration.getCapacityPrefetchCache();
              }
            }),
        Collections.synchronizedMap(
            new LinkedHashMap<S3URI, ColumnMappers>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry eldest) {
                return this.size() > configuration.getCapacityMultiObjects();
              }
            }),
        Collections.synchronizedMap(
            new LinkedHashMap<String, String>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry eldest) {
                return this.size() > configuration.getCapacityMultiObjects();
              }
            }));
  }

  /**
   * Creates an instance of MultiObjectsBlockManager This constructor is used for dependency
   * injection
   *
   * @param objectClient the Object Client to use to fetch the data
   * @param configuration the configuration
   * @param metadata the metadata cache
   * @param ioBlocks the IOBlock cache
   * @param prefetchCache the prefetch cache
   * @param columnMappersStore store for parquet metadata column mappings
   * @param recentColumns recent parquet columns read
   */
  protected MultiObjectsBlockManager(
      @NonNull ObjectClient objectClient,
      @NonNull BlockManagerConfiguration configuration,
      Map<S3URI, CompletableFuture<ObjectMetadata>> metadata,
      Map<S3URI, AutoClosingCircularBuffer<IOBlock>> ioBlocks,
      Map<S3URI, AutoClosingCircularBuffer<PrefetchIOBlock>> prefetchCache,
      Map<S3URI, ColumnMappers> columnMappersStore,
      Map<String, String> recentColumns) {
    this.objectClient = objectClient;
    this.configuration = configuration;
    this.metadata = metadata;
    this.ioBlocks = ioBlocks;
    this.prefetchCache = prefetchCache;
  }

  /**
   * Returns a future to the metadata of the object.
   *
   * @param s3URI the S3URI of the object
   * @return the metadata of the object
   */
  public CompletableFuture<ObjectMetadata> getMetadata(S3URI s3URI) {
    if (metadata.containsKey(s3URI)) {
      CompletableFuture<ObjectMetadata> objectMetadata = metadata.get(s3URI);
      try {
        objectMetadata.join();
        return objectMetadata;
      } catch (Exception e) {
        // remove failed entry from cache
        LOG.error("Removing failed head request for {}", s3URI.getKey());
        metadata.remove(s3URI);
      }
    }

    LOG.debug("Issuing new Head request for {}", s3URI.getKey());
    metadata.put(
        s3URI,
        objectClient.headObject(
            HeadRequest.builder().bucket(s3URI.getBucket()).key(s3URI.getKey()).build()));
    return metadata.get(s3URI);
  }

  /**
   * Reads a byte from the underlying object
   *
   * @param pos The position to read
   * @param s3URI The S3URI of the object
   * @return an unsigned int representing the byte that was read
   */
  public int read(long pos, S3URI s3URI) throws IOException {
    return getBlockForPosition(pos, s3URI).getByte(pos);
  }

  /**
   * Reads request data into the provided buffer
   *
   * @param buffer buffer to read data into
   * @param offset start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @param s3URI the S3URI of the object
   * @return the total number of bytes read into the buffer
   */
  public int read(byte[] buffer, int offset, int len, long pos, S3URI s3URI) throws IOException {
    int numBytesRead = 0;
    long numBytesRemaining = len;
    long nextReadPos = pos;
    int nextReadOffset = offset;

    while (numBytesRemaining > 0) {

      // Reached EOF
      if (nextReadPos > getLastObjectByte(s3URI)) {
        return numBytesRead;
      }

      IOBlock ioBlock = getBlockForPosition(nextReadPos, numBytesRemaining, s3URI);
      // Calculate the absolute start position within the current block and the number of bytes
      // remaining in it
      long absolutStartPositionInsideBlock = nextReadPos - ioBlock.getStart();
      long bytesRemainingInBlock = ioBlock.size() - absolutStartPositionInsideBlock;
      // Check if the current block has enough bytes to satisfy the remaining read
      if (bytesRemainingInBlock < numBytesRemaining) {
        // Calculate the relative start position for the next block
        long relativeStartPositionForNextBlock = nextReadPos + bytesRemainingInBlock;
        if (relativeStartPositionForNextBlock <= getLastObjectByte(s3URI)) {
          // Calculate the number of bytes to read for the next block
          long bytesToReadAfterCurrentBlock = numBytesRemaining - bytesRemainingInBlock;
          // Prefetch async block if it doesn't exist
          prefechBlockForPositionIfNotExists(
              relativeStartPositionForNextBlock, bytesToReadAfterCurrentBlock, s3URI);
        }
      }
      int numBytesToRead = ioBlock.read(buffer, nextReadOffset, numBytesRemaining, nextReadPos);
      nextReadOffset += numBytesToRead;
      nextReadPos += numBytesToRead;
      numBytesRemaining -= numBytesToRead;
      numBytesRead += numBytesToRead;
    }

    return numBytesRead;
  }

  /**
   * Reads the last n bytes from the object.
   *
   * @param buf byte buffer to read into
   * @param off position of first read byte in the byte buffer
   * @param n length of data to read in bytes
   * @param s3URI the S3URI of the object
   * @return the number of bytes read or -1 when EOF is reached
   */
  public int readTail(byte[] buf, int off, int n, S3URI s3URI) throws IOException {
    Preconditions.checkArgument(0 <= n, "must request a non-negative number of bytes from tail");
    Preconditions.checkArgument(
        n <= contentLength(s3URI),
        "cannot request more bytes from tail than total number of bytes");
    long start = contentLength(s3URI) - n;
    return read(buf, off, n, start, s3URI);
  }

  private IOBlock getBlockForPosition(long pos, long len, S3URI s3URI) throws IOException {
    Optional<IOBlock> lookup = lookupBlockForPosition(pos, s3URI);
    if (!lookup.isPresent()) {
      return createBlockStartingAtWithSize(pos, len, s3URI);
    }

    return lookup.get();
  }

  private void prefechBlockForPositionIfNotExists(long pos, long len, S3URI s3URI)
      throws IOException {
    Optional<IOBlock> lookup = lookupBlockForPosition(pos, s3URI);
    if (!lookup.isPresent()) {
      createPrefetchBlockAtWithSize(pos, len, s3URI);
    }
  }

  private IOBlock getBlockForPosition(long pos, S3URI s3URI) throws IOException {
    Optional<IOBlock> lookup = lookupBlockForPosition(pos, s3URI);
    if (!lookup.isPresent()) {
      return createBlockStartingAt(pos, s3URI);
    }
    return lookup.get();
  }

  private Optional<IOBlock> lookupBlockForPosition(long pos, S3URI s3URI) {
    // First check the prefetch cache
    AutoClosingCircularBuffer<PrefetchIOBlock> prefetchBlocks =
        prefetchCache.computeIfAbsent(
            s3URI, block -> new AutoClosingCircularBuffer<>(configuration.getCapacityBlocks()));
    Optional<PrefetchIOBlock> prefetchBlock = prefetchBlocks.findItem(block -> block.contains(pos));
    if (prefetchBlock.isPresent() && prefetchBlock.get().getIOBlock().isPresent()) {
      return prefetchBlock.get().getIOBlock();
    }
    // Block not present in the prefetch cache. Fetch it synchronously
    AutoClosingCircularBuffer<IOBlock> syncBlocks =
        ioBlocks.computeIfAbsent(
            s3URI, block -> new AutoClosingCircularBuffer<>(configuration.getCapacityBlocks()));
    return syncBlocks.findItem(block -> block.contains(pos));
  }

  private IOBlock createBlockStartingAt(long start, S3URI s3URI) throws IOException {
    long end = Math.min(start + configuration.getBlockSizeBytes() - 1, getLastObjectByte(s3URI));

    return createBlock(start, end, s3URI, false);
  }

  private IOBlock createBlockStartingAtWithSize(long start, long size, S3URI s3URI)
      throws IOException {
    long end;

    if (size > configuration.getReadAheadBytes()) {
      end = Math.min(start + size - 1, getLastObjectByte(s3URI));
    } else {
      end = Math.min(start + configuration.getReadAheadBytes() - 1, getLastObjectByte(s3URI));
    }

    return createBlock(start, end, s3URI, false);
  }

  private CompletableFuture<IOBlock> createPrefetchBlockAtWithSize(
      long start, long size, S3URI s3URI) throws IOException {
    long end;

    if (size > configuration.getReadAheadBytes()) {
      end = Math.min(start + size - 1, getLastObjectByte(s3URI));
    } else {
      end = Math.min(start + configuration.getReadAheadBytes() - 1, getLastObjectByte(s3URI));
    }

    return createPrefetchBlock(start, end, s3URI);
  }

  private IOBlock createBlock(long start, long end, S3URI s3URI, boolean isPrefetch)
      throws IOException {

    LOG.debug(
        "Creating IOBlock {}:{}. Object size {}. Object key {}. isPrefetch {}",
        start,
        end,
        contentLength(s3URI),
        s3URI.getKey(),
        isPrefetch);

    Range range = new Range(OptionalLong.of(start), OptionalLong.of(end));

    CompletableFuture<ObjectContent> objectContent =
        this.objectClient.getObject(
            GetRequest.builder()
                .bucket(s3URI.getBucket())
                .key(s3URI.getKey())
                .range(range)
                .referrer(new Referrer(range.toString(), isPrefetch).toString())
                .build());

    IOBlock ioBlock = new IOBlock(start, end, objectContent);
    if (!isPrefetch) {
      AutoClosingCircularBuffer<IOBlock> blocks =
          ioBlocks.computeIfAbsent(
              s3URI, block -> new AutoClosingCircularBuffer<>(configuration.getCapacityBlocks()));
      blocks.add(ioBlock);
    }
    return ioBlock;
  }

  private long contentLength(S3URI s3URI) {
    return this.getMetadata(s3URI).join().getContentLength();
  }

  private long getLastObjectByte(S3URI s3URI) {
    return contentLength(s3URI) - 1;
  }

  @Override
  public void close() throws IOException {
    this.ioBlocks.forEach(
        (key, block) -> {
          try {
            block.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
    this.ioBlocks.clear();
    this.prefetchCache.forEach(
        (key, asyncBlocks) -> {
          try {
            asyncBlocks.close();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
    this.prefetchCache.clear();
  }

  /**
   * Queue a prefetch request for the given ranges and for a specified by s3URI object . The request
   * will be processed asynchronously.
   *
   * @param prefetchRanges the ranges to prefetch
   * @param s3URI the S3URI of the object
   * @return a list of CompletableFuture<IOBlock> representing the prefetch requests
   */
  public List<CompletableFuture<IOBlock>> queuePrefetch(
      final List<com.amazon.connector.s3.io.physical.plan.Range> prefetchRanges,
      final S3URI s3URI) {
    List<CompletableFuture<IOBlock>> results = new ArrayList<>();
    prefetchRanges.forEach(
        range -> {
          long start = Math.max(0, range.getStart());
          long end = Math.max(0, range.getEnd());
          Optional<IOBlock> startBlock = lookupBlockForPosition(start, s3URI);
          if (startBlock.isPresent()) {
            return;
          }

          // TODO: implement caching strategy :
          // https://app.asana.com/0/1206885953994785/1207634179483967/f
          if (start > end || end > getLastObjectByte(s3URI)) {
            LOG.error(
                "Wrong range in queuePrefetch {}:{}. Content length: {}. Object {}.",
                start,
                end,
                contentLength(s3URI),
                s3URI.getKey());
            return;
          }

          try {
            results.add(createPrefetchBlock(start, end, s3URI));
          } catch (IOException e) {
            LOG.error(
                "Error in prefetching block for range: {}; key: {}; exception: {}",
                range,
                s3URI.getKey(),
                e);
            throw new RuntimeException(e);
          }
        });
    return results;
  }

  private CompletableFuture<IOBlock> createPrefetchBlock(long start, long end, S3URI s3URI)
      throws IOException {
    CompletableFuture<IOBlock> completableFutureIOBlock =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return this.createBlock(start, end, s3URI, true);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    PrefetchIOBlock prefetchIOBlock = new PrefetchIOBlock(start, end, completableFutureIOBlock);
    AutoClosingCircularBuffer<PrefetchIOBlock> prefetchBlocks =
        prefetchCache.computeIfAbsent(
            s3URI,
            block ->
                new AutoClosingCircularBuffer<PrefetchIOBlock>(configuration.getCapacityBlocks()));
    prefetchBlocks.add(prefetchIOBlock);
    return completableFutureIOBlock;
  }
}
