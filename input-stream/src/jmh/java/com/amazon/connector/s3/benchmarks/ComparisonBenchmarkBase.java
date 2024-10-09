package com.amazon.connector.s3.benchmarks;

import com.amazon.connector.s3.access.*;
import java.io.IOException;

/** Base class for benchmarks that compare performance of different clients */
public abstract class ComparisonBenchmarkBase extends BenchmarkBase {
  /**
   * Benchmarks can override this to return the {@link S3InputStreamKind}
   *
   * @return {@link S3InputStreamKind}
   */
  protected abstract S3InputStreamKind getS3InputStreamKind();

  /**
   * Runs the benchmark for a given pattern and object
   *
   * @throws IOException IO error, if encountered
   */
  @Override
  protected void executeBenchmark() throws IOException {
    switch (this.getS3InputStreamKind()) {
      case S3_DAT_GET:
        this.executeReadPatternOnDAT();
        break;
      case S3_SDK_GET:
        this.executeReadPatternDirectly();
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported S3 input kind: " + this.getS3InputStreamKind());
    }
  }
}
