package com.amazon.connector.s3.benchmarks.data.generation;

import com.amazon.connector.s3.benchmarks.common.BenchmarkContext;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** Generator kind */
@AllArgsConstructor
@Getter
public enum BenchmarkObjectGeneratorKind {
  RANDOM_SEQUENTIAL("sequential"),
  RANDOM_PARQUET("parquet");

  private final String value;

  /**
   * Creates a generator for a given context and generator kind
   *
   * @param context generator context
   * @return a new instance of {@link BenchmarkObjectGenerator}
   */
  public BenchmarkObjectGenerator createGenerator(BenchmarkContext context) {
    switch (this) {
      case RANDOM_SEQUENTIAL:
        return new RandomSequentialObjectGenerator(context);
      default:
        throw new IllegalArgumentException("Unsupported kind: " + this);
    }
  }
}
