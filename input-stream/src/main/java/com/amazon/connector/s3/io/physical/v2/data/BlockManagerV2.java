package com.amazon.connector.s3.io.physical.v2.data;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.io.physical.v1.blockmanager.BlockManagerConfiguration;
import com.amazon.connector.s3.io.physical.v2.prefetcher.SequentialPatternDetector;
import com.amazon.connector.s3.io.physical.v2.prefetcher.SequentialReadProgression;
import com.amazon.connector.s3.util.S3URI;
import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/** Implements a Block Manager responsible for planning and scheduling reads on a key. */
public class BlockManagerV2 implements Closeable {

  private final S3URI s3URI;
  private final MetadataStore metadataStore;
  private final BlockStore blockStore;
  private final ObjectClient objectClient;
  private final SequentialPatternDetector patternDetector;
  private final SequentialReadProgression sequentialReadProgression;
  private final IOPlanner ioPlanner;

  /**
   * Constructs a new BlockManager.
   *
   * @param s3URI the S3 URI of the object
   * @param objectClient object client capable of interacting with the underlying object store
   * @param metadataStore the metadata cache
   */
  public BlockManagerV2(S3URI s3URI, ObjectClient objectClient, MetadataStore metadataStore) {
    this.s3URI = s3URI;
    this.objectClient = objectClient;
    this.metadataStore = metadataStore;
    this.blockStore = new BlockStore(s3URI, metadataStore);
    this.patternDetector = new SequentialPatternDetector(blockStore);
    this.sequentialReadProgression = new SequentialReadProgression();
    this.ioPlanner = new IOPlanner(blockStore);
  }

  /**
   * Given the position of a byte, return the block holding it.
   *
   * @param pos the position of a byte
   * @return the Block holding the byte or empty if the byte is not in the BlockStore
   */
  public synchronized Optional<Block> getBlock(long pos) {
    return this.blockStore.getBlock(pos);
  }

  /**
   * Make sure that the byte at a give position is in the BlockStore.
   *
   * @param pos the position of the byte
   */
  public synchronized void makePositionAvailable(long pos) {
    // Position is already available --> return corresponding block
    if (getBlock(pos).isPresent()) {
      return;
    }

    makeRangeAvailable(pos, 1);
  }

  private boolean isRangeAvailable(long pos, long len) {
    long lastByteOfRange = pos + len - 1;

    if (blockStore.findNextMissingByte(pos).isPresent()) {
      return lastByteOfRange < blockStore.findNextMissingByte(pos).getAsLong();
    }

    // If there is no missing byte after pos, then the whole object is already fetched
    return true;
  }

  /**
   * Method that ensures that a range is fully available in the object store. After calling this
   * method the BlockStore should contain all bytes in the range and we should be able to service a
   * read through the BlockStore.
   *
   * @param pos start of a read
   * @param len length of the read
   */
  public synchronized void makeRangeAvailable(long pos, long len) {
    if (isRangeAvailable(pos, len)) {
      return;
    }

    // TODO: use the proper value from the configuration
    len = Math.max(len, BlockManagerConfiguration.DEFAULT_READ_AHEAD_BYTES);

    // In case of a sequential reading pattern, calculate the generation and adjust the requested
    // end of the requested range
    long end = pos + len - 1;
    final long generation;

    if (patternDetector.isSequentialRead(pos)) {
      generation = patternDetector.getGeneration(pos);
      long newSize = sequentialReadProgression.getSizeForGeneration(generation);
      end = truncatePos(pos + newSize);
    } else {
      generation = 0;
    }

    // Determine the missing ranges and fetch them
    List<Range> missingRanges = ioPlanner.planRead(pos, end, getLastObjectByte());
    List<Range> splits = RangeSplitter.splitRanges(missingRanges);
    splits.forEach(
        r -> {
          Block block = new Block(s3URI, objectClient, r.getStart(), r.getEnd(), generation);
          blockStore.add(block);
        });
  }

  private long getLastObjectByte() {
    return this.metadataStore.get(s3URI).join().getContentLength() - 1;
  }

  private long truncatePos(long pos) {
    return Math.min(pos, getLastObjectByte());
  }

  @Override
  public void close() {
    blockStore.close();
  }
}
