package com.amazon.connector.s3.datagen;

import static com.amazon.connector.s3.datagen.Constants.ONE_GB_IN_BYTES;
import static com.amazon.connector.s3.datagen.Constants.ONE_MB_IN_BYTES;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

/**
 * Class defining objects used by micro-benchmarks and defining the shapes of some read patterns we
 * want to test for.
 */
public class BenchmarkData {

  /**
   * Class describing a benchmark object and helper methods defining read patterns (forward seeking,
   * backward seeking, parquet-like jumps) on the object.
   */
  @AllArgsConstructor
  @Getter
  public enum BenchmarkObject {
    RANDOM_1MB("random-1mb.txt", 1 * ONE_MB_IN_BYTES),
    RANDOM_4MB("random-4mb.txt", 4 * ONE_MB_IN_BYTES),
    RANDOM_16MB("random-16mb.txt", 16 * ONE_MB_IN_BYTES),
    RANDOM_64MB("random-64mb.txt", 64 * ONE_MB_IN_BYTES),
    RANDOM_128MB("random-128mb.txt", 128 * ONE_MB_IN_BYTES),
    RANDOM_256MB("random-256mb.txt", 256 * ONE_MB_IN_BYTES),
    RANDOM_1G("random-1G.txt", ONE_GB_IN_BYTES),
    RANDOM_5G("random-5G.txt", 5L * ONE_GB_IN_BYTES);

    private final String keyName;
    private final long size;

    /**
     * @return a read pattern that 'jumps through the object' forward by 20% increments and reads
     *     10% of the object each time. Essentially, this results half of the content being read and
     *     half of the content being ignored.
     *     <p>Illustration of the pattern (number denotes order of read):
     *     1111111111---2222222222---3333333333--- ... --- 5555555555--- | 0% | 20% | 40 % | 80% |
     *     100%
     */
    public List<Read> getForwardSeekReadPattern() {
      return Stream.of(
              Read.builder().start(0).length(percent(10)).build(),
              Read.builder().start(percent(20)).length(percent(10)).build(),
              Read.builder().start(percent(40)).length(percent(10)).build(),
              Read.builder().start(percent(60)).length(percent(10)).build(),
              Read.builder().start(percent(80)).length(percent(10)).build())
          .collect(Collectors.toList());
    }

    /**
     * @return a read pattern that does backward seeks
     */
    public List<Read> getBackwardSeekReadPattern() {
      List<Read> result = new ArrayList<>(getForwardSeekReadPattern());
      Collections.reverse(result);
      return result;
    }

    /**
     * Define a tail dance + read 50% of the object.
     *
     * @return a read pattern which is Parquet-like
     */
    public List<Read> getParquetLikeReadPattern() {
      return Stream.of(
              // Tail dance
              Read.builder().start(size - 1 - 4).length(4).build(),
              Read.builder()
                  .start(size - 8 * Constants.ONE_KB_IN_BYTES)
                  .length(8 * Constants.ONE_KB_IN_BYTES)
                  .build(),

              // Read some contiguous chunks
              Read.builder().start(percent(50)).length(percent(30)).build(),
              Read.builder().start(0).length(percent(20)).build())
          .collect(Collectors.toList());
    }

    /**
     * @param x an integer between 0 and 100 representing a percentage
     * @return an integer representing the position in the object which is roughly x% of its size.
     *     This is used to seek into specific relative positions in the object when defining read
     *     patterns.
     */
    private long percent(int x) {
      return (size / 100) * x;
    }
  }

  /** Object representing a read. Has a start and a length. */
  @Data
  @Builder
  public static class Read {
    long start;
    long length;
  }
}
