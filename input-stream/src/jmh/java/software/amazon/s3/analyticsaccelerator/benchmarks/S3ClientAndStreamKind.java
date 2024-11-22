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

import lombok.AllArgsConstructor;
import lombok.Getter;
import software.amazon.s3.analyticsaccelerator.access.S3ClientKind;
import software.amazon.s3.analyticsaccelerator.access.S3InputStreamKind;

/** Client and stream kind combined. This simplifies benchmarking parameterization and reports */
@AllArgsConstructor
@Getter
public enum S3ClientAndStreamKind {
  SDK_ASYNC_JAVA(S3ClientKind.SDK_V2_JAVA_ASYNC, S3InputStreamKind.S3_SDK_GET),
  SDK_DAT_JAVA(S3ClientKind.SDK_V2_JAVA_ASYNC, S3InputStreamKind.S3_DAT_GET),
  SDK_ASYNC_CRT(S3ClientKind.SDK_V2_CRT_ASYNC, S3InputStreamKind.S3_SDK_GET),
  SDK_DAT_CRT(S3ClientKind.SDK_V2_CRT_ASYNC, S3InputStreamKind.S3_DAT_GET);

  private final S3ClientKind clientKind;
  private final S3InputStreamKind inputStreamKind;
}
