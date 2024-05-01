package com.amazon.connector.s3.datagen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import lombok.Builder;
import lombok.Data;

/**
 * Class defining objects used by microbenchmarks and defining the shapes of some read patterns we
 * want to test for.
 */
public class BenchmarkData {

  /**
   * Class describing a benchmark object and helper methods defining read patterns (forward seeking,
   * backward seeking, parquet-like jumps) on the object.
   */
  @Data
  @Builder
  public static class BenchmarkObject {
    private final String keyName;
    private final long size;

    /**
     * Returns a read pattern that 'jumps through the object' forward by 20% increments and reads
     * 10% of the object each time. Essentially, this results half of the content being read and
     * half of the content being ignored.
     *
     * <p>Illustration of the pattern (number denotes order of read):
     * 1111111111---2222222222---3333333333--- ... --- 5555555555--- | 0% | 20% | 40 % | 80% | 100%
     */
    public List<Read> getForwardSeekReadPattern() {
      return ImmutableList.of(
          Read.builder().start(0).length(percent(10)).build(),
          Read.builder().start(percent(20)).length(percent(10)).build(),
          Read.builder().start(percent(40)).length(percent(10)).build(),
          Read.builder().start(percent(60)).length(percent(10)).build(),
          Read.builder().start(percent(80)).length(percent(10)).build());
    }

    /** Just reverse the forward pattern */
    public List<Read> getBackwardSeekReadPattern() {
      return Lists.reverse(getForwardSeekReadPattern());
    }

    /** Define a tail dance + read 50% of the object */
    public List<Read> getParquetLikeReadPattern() {
      return ImmutableList.of(
          // Tail dance
          Read.builder().start(size - 1 - 4).length(4).build(),
          Read.builder()
              .start(size - 8 * Constants.ONE_KB_IN_BYTES)
              .length(8 * Constants.ONE_KB_IN_BYTES)
              .build(),

          // Read some contiguous chunks
          Read.builder().start(percent(50)).length(percent(30)).build(),
          Read.builder().start(0).length(percent(20)).build());
    }

    /**
     * Returns x% of an integer. Used above to seek into specific relative positions in the object
     * when defining read patterns.
     */
    private int percent(int x) {
      return (int) ((size / 100) * x);
    }
  }

  /** Object representing a read. Has a start and a length. */
  @Data
  @Builder
  public static class Read {
    long start;
    long length;
  }

  /**
   * (object-name, object) pairs -- we need this due to the limitation that JMH only allows us to
   * parameterise with Strings. So we use this Map under the hood to implement a keymap.
   */
  public static final List<BenchmarkObject> BENCHMARK_OBJECTS =
      ImmutableList.of(
          BenchmarkObject.builder()
              .keyName("random-1mb.txt")
              .size(1 * Constants.ONE_MB_IN_BYTES)
              .build(),
          BenchmarkObject.builder()
              .keyName("random-4mb.txt")
              .size(4 * Constants.ONE_MB_IN_BYTES)
              .build(),
          BenchmarkObject.builder()
              .keyName("random-16mb.txt")
              .size(16 * Constants.ONE_MB_IN_BYTES)
              .build(),
          BenchmarkObject.builder()
              .keyName("random-64mb.txt")
              .size(64 * Constants.ONE_MB_IN_BYTES)
              .build(),
          BenchmarkObject.builder()
              .keyName("random-128mb.txt")
              .size(128 * Constants.ONE_MB_IN_BYTES)
              .build(),
          BenchmarkObject.builder()
              .keyName("random-256mb.txt")
              .size(256 * Constants.ONE_MB_IN_BYTES)
              .build());

  /** Returns a benchmark object by name. */
  public static BenchmarkObject getBenchMarkObjectByName(String name) {
    return BENCHMARK_OBJECTS.stream()
        .filter(o -> o.getKeyName().equals(name))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Cannot find benchmark object"));
  }
}
