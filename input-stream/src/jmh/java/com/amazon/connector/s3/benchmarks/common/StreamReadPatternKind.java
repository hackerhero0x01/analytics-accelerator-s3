package com.amazon.connector.s3.benchmarks.common;

import com.amazon.connector.s3.benchmarks.data.generation.Constants;
import java.util.ArrayList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

/** Enum that describes different supported stream seeks */
@AllArgsConstructor
@Getter
public enum StreamReadPatternKind {
  SKIPPING_FORWARD("SKIPPING_FORWARD"),
  SKIPPING_BACKWARD("SKIPPING_BACKWARD"),
  QUASI_PARQUET_SIMPLE("QUASI_PARQUET_SIMPLE");
  private final String value;

  /**
   * Gets the stream read pattern based on the value of this enum
   *
   * @param benchmarkObject {@link BenchmarkObject} to read
   * @return the stream read pattern
   */
  public StreamReadPattern getStreamReadPattern(BenchmarkObject benchmarkObject) {
    switch (this) {
      case SKIPPING_FORWARD:
        return getForwardSeekReadPattern(benchmarkObject);
      case SKIPPING_BACKWARD:
        return getBackwardSeekReadPattern(benchmarkObject);
      case QUASI_PARQUET_SIMPLE:
        return getQuasiParqetSimpleReadPattern(benchmarkObject);
      default:
        throw new IllegalArgumentException("Unknown stream read pattern: " + this);
    }
  }

  /**
   * Construct a read pattern that 'jumps through the object' forward by 20% increments and reads
   * 10% of the object each time. Essentially, this results half of the content being read and half
   * of the content being ignored.
   *
   * @param benchmarkObject {@link BenchmarkObject} to read
   * @return a forward seeking read pattern
   */
  private static StreamReadPattern getForwardSeekReadPattern(
      @NonNull BenchmarkObject benchmarkObject) {
    return StreamReadPattern.builder()
        .streamRead(StreamRead.builder().start(0).length(percent(benchmarkObject, 10)).build())
        .streamRead(
            StreamRead.builder()
                .start(percent(benchmarkObject, 20))
                .length(percent(benchmarkObject, 10))
                .build())
        .streamRead(
            StreamRead.builder()
                .start(percent(benchmarkObject, 40))
                .length(percent(benchmarkObject, 10))
                .build())
        .streamRead(
            StreamRead.builder()
                .start(percent(benchmarkObject, 60))
                .length(percent(benchmarkObject, 10))
                .build())
        .streamRead(
            StreamRead.builder()
                .start(percent(benchmarkObject, 80))
                .length(percent(benchmarkObject, 10))
                .build())
        .build();
  }

  /**
   * Construct a read pattern that 'jumps through the object' forward by 20% increments and reads
   * 10% of the object each time. Essentially, this results half of the content being read and half
   * of the content being ignored.
   *
   * @param benchmarkObject {@link BenchmarkObject} to seek
   * @return a forward seeking read pattern
   */
  private static StreamReadPattern getBackwardSeekReadPattern(BenchmarkObject benchmarkObject) {
    return StreamReadPattern.builder()
        .streamReads(new ArrayList<>(getForwardSeekReadPattern(benchmarkObject).getStreamReads()))
        .build();
  }

  /**
   * Define a tail dance + read 50% of the object.
   *
   * @param benchmarkObject {@link BenchmarkObject} to read
   * @return a read pattern which is Parquet-like
   */
  private static StreamReadPattern getQuasiParqetSimpleReadPattern(
      BenchmarkObject benchmarkObject) {
    return StreamReadPattern.builder()
        // Footer size read
        .streamRead(StreamRead.builder().start(benchmarkObject.getSize() - 1 - 4).length(4).build())
        // Footer read
        .streamRead(
            StreamRead.builder()
                .start(benchmarkObject.getSize() - 8 * Constants.ONE_KB_IN_BYTES)
                .length(8 * Constants.ONE_KB_IN_BYTES)
                .build())
        // read a few contiguous chunks
        .streamRead(
            StreamRead.builder()
                .start(percent(benchmarkObject, 50))
                .length(percent(benchmarkObject, 30))
                .build())
        .streamRead(
            StreamRead.builder()
                .start(percent(benchmarkObject, 0))
                .length(percent(benchmarkObject, 220))
                .build())
        .build();
  }

  /**
   * @param benchmarkObject benchmark object
   * @param percent an integer between 0 and 100 representing a percentage
   * @return an integer representing the position in the object which is roughly x% of its size.
   *     This is used to seek into specific relative positions in the object when defining read
   *     patterns.
   */
  private static long percent(BenchmarkObject benchmarkObject, int percent) {
    return (benchmarkObject.getSize() / 100) * percent;
  }
}
