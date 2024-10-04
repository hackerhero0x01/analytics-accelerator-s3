package com.amazon.connector.s3.benchmarks.data.generation;

import com.amazon.connector.s3.benchmarks.common.BenchmarkContext;
import com.amazon.connector.s3.util.S3URI;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/** Based class for all generators */
@Getter
@RequiredArgsConstructor
public abstract class BenchmarkObjectGenerator {
  @NonNull private final BenchmarkContext context;
  @NonNull private final BenchmarkObjectGeneratorKind kind;

  /**
   * Generate data
   *
   * @param s3URI S3 URI to generate data into
   * @param size object size
   */
  public abstract void generate(S3URI s3URI, long size);
}
