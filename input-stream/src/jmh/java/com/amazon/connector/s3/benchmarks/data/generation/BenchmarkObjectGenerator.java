package com.amazon.connector.s3.benchmarks.data.generation;

import com.amazon.connector.s3.access.S3ExecutionContext;
import com.amazon.connector.s3.access.S3ObjectKind;
import com.amazon.connector.s3.util.S3URI;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/** Base class for all generators */
@Getter
@RequiredArgsConstructor
public abstract class BenchmarkObjectGenerator {
  @NonNull private final S3ExecutionContext context;
  @NonNull private final S3ObjectKind kind;

  /**
   * Generate data
   *
   * @param s3URI S3 URI to generate data into
   * @param size object size
   */
  public abstract void generate(S3URI s3URI, long size);
}
