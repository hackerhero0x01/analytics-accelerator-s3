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
import software.amazon.s3.analyticsaccelerator.access.S3InputStreamKind;

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
      case S3_AAL_GET:
        this.executeReadPatternOnAAL();
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
