  /*
   * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   *
   * Licensed under the Apache License, Version 2.0 (the "License").
   * You may not use this file except in compliance with the License.
   * You may obtain a copy of the License at
   *
   *   http://www.apache.org/licenses/LICENSE-2.0
  
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
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
 * determine which combo is the fastest for each pattern.
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
