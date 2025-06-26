/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.benchmarks;

import java.io.IOException;
import org.openjdk.jmh.annotations.*;
import software.amazon.s3.analyticsaccelerator.access.S3ClientKind;
import software.amazon.s3.analyticsaccelerator.access.S3Object;
import software.amazon.s3.analyticsaccelerator.access.StreamReadPatternKind;

/**
 * Benchmarks that measure performance of AAL via CRT by replaying all patterns against multiple
 * object sizes
 */
public class AALBenchmark extends BenchmarkBase {
  // NOTE: all params here must come after "object", so they should start with any letter after "o".
  @Param public StreamReadPatternKind pattern;

  /**
   * Runs the benchmark for a given pattern and object
   *
   * @throws IOException IO error, if encountered
   */
  @Override
  protected void executeBenchmark() throws IOException {
    executeReadPatternOnAAL();
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
