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
package software.amazon.s3.dataaccelerator.benchmarks.data.generation;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import software.amazon.s3.dataaccelerator.access.S3ExecutionContext;
import software.amazon.s3.dataaccelerator.access.S3ObjectKind;
import software.amazon.s3.dataaccelerator.util.S3URI;

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
