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
package software.amazon.s3.dataaccelerator.access;

import lombok.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.s3.dataaccelerator.common.ConnectorConfiguration;

/** Configuration for S3 Async Clients used by the benchmarks */
@Value
@Builder
public class S3AsyncClientFactoryConfiguration {
  public static final String REGION_KEY = "S3_TEST_REGION";
  public static final String CRT_PART_SIZE_BYTES_KEY = "S3_TEST_CRT_PART_SIZE_BYTES";
  public static final String CRT_NATIVE_MEMORY_BYTES_KEY = "S3_TEST_CRT_NATIVE_MEMORY_BYTES";
  public static final String CRT_MAX_CONCURRENCY_KEY = "S3_TEST_CRT_MAX_CONCURRENCY_BYTES";
  public static final String CRT_CHECKSUM_VALIDATION_ENABLED_KEY =
      "S3_TEST_CRT_CHECKSUM_VALIDATION_ENABLED";
  public static final String CRT_READ_BUFFER_SIZE_BYTES_KEY = "S3_TEST_CRT_READ_BUFFER_SIZE_BYTES";
  public static final String CRT_TARGET_THROUGHPUT_GBPS_KEY = "S3_TEST_CRT_TARGET_THROUGHPUT_GBPS";

  public static final Region DEFAULT_REGION = Region.US_EAST_1;
  public static final long DEFAULT_CRT_PART_SIZE_BYTES = 8 * 1024 * 1024;
  public static final long DEFAULT_CRT_NATIVE_MEMORY_BYTES = 1024 * 1024 * 1024;
  public static final int DEFAULT_CRT_MAX_CONCURRENCY = 300;
  public static final boolean DEFAULT_CHECKSUM_VALIDATION_ENABLED = true;
  public static final long DEFAULT_CRT_READ_BUFFER_SIZE_BYTES = 10 * DEFAULT_CRT_PART_SIZE_BYTES;
  public static final int DEFAULT_CRT_TARGET_THROUGHPUT_GBPS = 100;

  @NonNull Region region;
  long crtPartSizeInBytes;
  long crtNativeMemoryLimitInBytes;
  int crtMaxConcurrency;
  boolean crtChecksumValidationEnabled;
  long crtReadBufferSizeInBytes;
  int crtTargetThroughputGbps;

  /**
   * Creates the {@link S3AsyncClientFactoryConfiguration} from the supplied configuration
   *
   * @param configuration an instance of configuration
   * @return anew instance of {@link S3AsyncClientFactoryConfiguration}
   */
  public static S3AsyncClientFactoryConfiguration fromConfiguration(
      ConnectorConfiguration configuration) {
    return S3AsyncClientFactoryConfiguration.builder()
        .region(Region.of(configuration.getString(REGION_KEY, DEFAULT_REGION.id())))
        .crtPartSizeInBytes(
            configuration.getLong(CRT_PART_SIZE_BYTES_KEY, DEFAULT_CRT_PART_SIZE_BYTES))
        .crtNativeMemoryLimitInBytes(
            configuration.getLong(CRT_NATIVE_MEMORY_BYTES_KEY, DEFAULT_CRT_NATIVE_MEMORY_BYTES))
        .crtMaxConcurrency(
            configuration.getInt(CRT_MAX_CONCURRENCY_KEY, DEFAULT_CRT_MAX_CONCURRENCY))
        .crtChecksumValidationEnabled(
            configuration.getBoolean(
                CRT_CHECKSUM_VALIDATION_ENABLED_KEY, DEFAULT_CHECKSUM_VALIDATION_ENABLED))
        .crtReadBufferSizeInBytes(
            configuration.getLong(
                CRT_READ_BUFFER_SIZE_BYTES_KEY, DEFAULT_CRT_READ_BUFFER_SIZE_BYTES))
        .crtTargetThroughputGbps(
            configuration.getInt(
                CRT_TARGET_THROUGHPUT_GBPS_KEY, DEFAULT_CRT_TARGET_THROUGHPUT_GBPS))
        .build();
  }
}
