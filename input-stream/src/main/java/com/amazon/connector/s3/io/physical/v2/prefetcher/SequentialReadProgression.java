package com.amazon.connector.s3.io.physical.v2.prefetcher;

import static com.amazon.connector.s3.util.Constants.ONE_MB;

/**
 * Class that implements a mathematical function telling us the size of blocks we should prefetch in
 * a sequential read.
 */
public class SequentialReadProgression {

  // TODO: we may want to expose these as customer configurable
  private static double BASE = 4.0;
  private static double ALPHA = 1;

  /**
   * Given a generation, returns the size of a sequential prefetch block for that generation. This
   * function is effectively a geometric series today but can be fine-tuned later.
   *
   * @param generation zero-indexed integer representing the generation of a read
   * @return a block size in bytes
   */
  public long getSizeForGeneration(long generation) {
    // 2, 8, 32, 64
    return 2 * ONE_MB * (long) Math.pow(BASE, Math.floor(ALPHA * generation));
  }
}
