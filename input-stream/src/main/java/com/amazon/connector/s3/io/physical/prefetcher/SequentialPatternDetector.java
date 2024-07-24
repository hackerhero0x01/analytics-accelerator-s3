package com.amazon.connector.s3.io.physical.prefetcher;

import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.io.physical.data.BlockStore;
import lombok.RequiredArgsConstructor;

/**
 * Class capable of detecting sequential read patterns.
 *
 * <p>The SequentialPatternDetector depends on the BlockStore which it is capable of 'inspecting'.
 * For now, pattern detection is very simple: if the previous byte is already stored by a block,
 * then we conclude that the byte is requested by a sequential read. In the future we should
 * probably extend this to work with some kind of tolerance or radius (i.e., look back more than a
 * single byte).
 *
 * <p>The other responsibility of this class is to compute what the 'generation' of a position is in
 * the BlockStore.
 */
@RequiredArgsConstructor
public class SequentialPatternDetector {

  private final BlockStore blockStore;

  /**
   * Given that the byte at 'pos' will be read next, is this read part of a sequential read?
   *
   * @param pos the position of a byte that is requested
   * @return returns true if this read is part of a sequential read pattern
   */
  public boolean isSequentialRead(long pos) {
    Preconditions.checkArgument(pos >= 0, "position must be bigger than or equal to 0");

    if (pos == 0) {
      return false;
    }

    return blockStore.getBlock(pos - 1).isPresent();
  }

  /**
   * Given that the byte at 'pos' will be read next, what generation does it belong to?
   *
   * @param pos the position of a byte that is requested
   * @return returns the generation of the byte
   */
  public long getGeneration(long pos) {
    Preconditions.checkArgument(pos >= 0, "position must be bigger than or equal to 0");

    if (isSequentialRead(pos)) {
      return blockStore.getBlock(pos - 1).get().getGeneration() + 1;
    }

    return 0;
  }
}
