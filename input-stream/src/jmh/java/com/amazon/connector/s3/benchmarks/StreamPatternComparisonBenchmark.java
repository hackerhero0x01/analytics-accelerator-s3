package com.amazon.connector.s3.benchmarks;

import com.amazon.connector.s3.access.S3ClientKind;
import com.amazon.connector.s3.access.S3InputStreamKind;
import com.amazon.connector.s3.access.S3Object;
import com.amazon.connector.s3.access.StreamReadPatternKind;
import lombok.NonNull;
import org.openjdk.jmh.annotations.Param;

/**
 * This benchmarks fixes the ReadPattern, and then, for each object size, replays the pattern on
 * "raw" clients as well as "DAT" streams that sit on the clients The main point here is to
 * determine which combo is the fastest for each pattern
 */
public abstract class StreamPatternComparisonBenchmark extends ComparisonBenchmarkBase {
  // NOTE: all params here must come after "object", so they should start with any letter after "o".
  @Param public S3ClientAndStreamKind s3ClientAndStreamKind;

  @NonNull private final StreamReadPatternKind patternKind;

  /**
   * Creates a new instance of {@link StreamPatternComparisonBenchmark}
   *
   * @param patternKind pattern kind
   */
  protected StreamPatternComparisonBenchmark(@NonNull StreamReadPatternKind patternKind) {
    this.patternKind = patternKind;
  }

  @Override
  protected S3InputStreamKind getS3InputStreamKind() {
    return this.s3ClientAndStreamKind.getInputStreamKind();
  }

  @Override
  protected S3Object getObject() {
    return this.object;
  }

  @Override
  protected StreamReadPatternKind getReadPatternKind() {
    return this.patternKind;
  }

  @Override
  protected S3ClientKind getClientKind() {
    return this.s3ClientAndStreamKind.getClientKind();
  }
}
