  /*
   * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   *
   * Licensed under the Apache License, Version 2.0 (the "License").
   * You may not use this file except in compliance with the License.
   * You may obtain a copy of the License at
   *
   *   http://www.apache.org/licenses/LICENSE-2.0
  
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
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
    Preconditions.checkArgument(pos >= 0, "`pos` must be non-negative");

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
    Preconditions.checkArgument(pos >= 0, "`pos` must be non-negative");

    if (isSequentialRead(pos)) {
      return blockStore.getBlock(pos - 1).get().getGeneration() + 1;
    }

    return 0;
  }
}
