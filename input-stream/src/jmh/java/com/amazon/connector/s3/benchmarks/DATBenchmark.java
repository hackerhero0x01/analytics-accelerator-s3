package com.amazon.connector.s3.benchmarks;

import com.amazon.connector.s3.access.S3ClientKind;
import com.amazon.connector.s3.access.S3Object;
import com.amazon.connector.s3.access.StreamReadPatternKind;
import java.io.IOException;
import org.openjdk.jmh.annotations.*;

/**
 * Benchmarks that measure performance of DAT via CRT by replaying all patterns against multiple
 * object sizes
 */
public class DATBenchmark extends BenchmarkBase {
  // NOTE: all params here must come after "object", so they should start with any letter after "o".
  @Param public StreamReadPatternKind pattern;

  /**
   * Runs the benchmark for a given pattern and object
   *
   * @throws IOException IO error, if encountered
   */
  @Override
  protected void executeBenchmark() throws IOException {
    executeReadPatternOnDAT();
  }

  /**
   * S3 Client Kind
   *
   * @return {@link S3ClientKind}
   */
  @Override
  protected S3ClientKind getClientKind() {
    // Always use CRT client
    return S3ClientKind.SDK_V2_CRT_ASYNC;
  }

  @Override
  protected S3Object getObject() {
    return object;
  }

  @Override
  protected StreamReadPatternKind getReadPatternKind() {
    return pattern;
  }
}
