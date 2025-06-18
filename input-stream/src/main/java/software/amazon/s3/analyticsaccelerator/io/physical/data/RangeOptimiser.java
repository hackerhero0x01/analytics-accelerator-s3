/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.io.physical.data;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import lombok.Value;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.Range;

/**
 * RangeSplitter is responsible for splitting up big ranges into smaller reads. The need for such
 * functionality arises from sequential prefetching. When we decide that, e.g., the next 128MB chunk
 * of an object is needed with high confidence, then we should not fetch this in a single request.
 *
 * <p>This class is capable of implementing heuristics on how to fetch ranges of different sizes
 * optimally.
 */
@Value
public class RangeOptimiser {
  PhysicalIOConfiguration configuration;

  /**
   * Given a list of ranges, return a potentially new set of ranges which is more optimal to fetch
   * (i.e., split up huge ranges based on a heuristic).
   *
   * @param ranges a list of ranges
   * @return a potentially different list of ranges with big ranges split up
   */
  public List<Range> splitRanges(List<Range> ranges) {
    List<Range> splits = new LinkedList<>();
    for (Range range : ranges) {
      if (range.getLength() > configuration.getMaxRangeSizeBytes()) {
        splitRange(range.getStart(), range.getEnd()).forEach(splits::add);
      } else {
        splits.add(range);
      }
    }

    return splits;
  }

  private List<Range> splitRange(long start, long end) {
    long nextRangeStart = start;
    List<Range> generatedRanges = new LinkedList<>();

    while (nextRangeStart < end) {
      long rangeEnd = Math.min(nextRangeStart + configuration.getPartSizeBytes() - 1, end);
      generatedRanges.add(new Range(nextRangeStart, rangeEnd));
      nextRangeStart = rangeEnd + 1;
    }

    return generatedRanges;
  }

  /**
   * Groups sequential block indexes into separate lists, ensuring each group doesn't exceed the
   * maximum block count. This method optimizes read operations by: 1. First grouping blocks by
   * sequential indexes (blocks with consecutive numbers) 2. Then splitting any large sequential
   * groups that exceed maxRangeBlocks into smaller chunks of partSizeBlocks
   *
   * <p>Example 1 - Basic sequential grouping: Input: [1,2,3,5,6,8,9,10] Output:
   * [[1,2,3],[5,6],[8,9,10]] (Blocks are grouped by sequential indexes regardless of size limits)
   *
   * <p>Example 2 - Size-based splitting: Input: [1,2,3,4,5,6,7,8,9,10] With maxRangeBlocks=4 and
   * partSizeBlocks=3: Output: [[1,2,3], [4,5,6], [7,8,9], [10]] (Since the sequential group exceeds
   * maxRangeBlocks=4, it's split into chunks of partSizeBlocks=3)
   *
   * <p>Example 3 - Mixed sequential and size-based splitting: Input:
   * [1,2,3,4,5,6,10,11,12,13,14,15,16,17] With maxRangeBlocks=3 and partSizeBlocks=2: Output:
   * [[1,2], [3,4], [5,6], [10,11], [12,13], [14,15], [16,17]] (Each sequential group exceeds
   * maxRangeBlocks=3, so each is split into chunks of partSizeBlocks=2)
   *
   * @param blockIndexes an ordered list of block indexes
   * @param readBufferSize size of each block in bytes
   * @return a list of lists where each inner list contains sequential block indexes within size
   *     limits
   */
  public List<List<Integer>> optimizeReads(List<Integer> blockIndexes, long readBufferSize) {
    List<List<Integer>> result = new ArrayList<>();
    if (blockIndexes == null || blockIndexes.isEmpty()) {
      return result;
    }

    // Calculate max blocks per read based on configuration values
    int maxRangeBlocks = (int) (configuration.getMaxRangeSizeBytes() / readBufferSize);
    int partSizeBlocks = (int) (configuration.getPartSizeBytes() / readBufferSize);

    // Ensure at least one block per read
    maxRangeBlocks = Math.max(1, maxRangeBlocks);
    partSizeBlocks = Math.max(1, partSizeBlocks);

    // First, group by sequential blocks
    List<List<Integer>> sequentialGroups = new ArrayList<>();
    List<Integer> currentSequence = new ArrayList<>();
    currentSequence.add(blockIndexes.get(0));

    for (int i = 1; i < blockIndexes.size(); i++) {
      int current = blockIndexes.get(i);
      int previous = blockIndexes.get(i - 1);

      if (current == previous + 1) {
        // Sequential index, add to current sequence
        currentSequence.add(current);
      } else {
        // Non-sequential index, start a new sequence
        sequentialGroups.add(currentSequence);
        currentSequence = new ArrayList<>();
        currentSequence.add(current);
      }
    }

    // Add the last sequence if not empty
    if (!currentSequence.isEmpty()) {
      sequentialGroups.add(currentSequence);
    }

    // Now split each sequential group if it exceeds maxRangeBlocks
    for (List<Integer> group : sequentialGroups) {
      if (group.size() <= maxRangeBlocks) {
        // Group is within size limit, add as is
        result.add(group);
      } else {
        // Group exceeds size limit, split into partSizeBlocks chunks
        for (int j = 0; j < group.size(); j += partSizeBlocks) {
          List<Integer> chunk = new ArrayList<>();
          for (int k = j; k < j + partSizeBlocks && k < group.size(); k++) {
            chunk.add(group.get(k));
          }
          result.add(chunk);
        }
      }
    }

    return result;
  }
}
