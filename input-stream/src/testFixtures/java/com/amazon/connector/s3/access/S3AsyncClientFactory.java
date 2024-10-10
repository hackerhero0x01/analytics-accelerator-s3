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
package com.amazon.connector.s3.access;

import lombok.NonNull;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/** Small factory that creates the Async client */
public class S3AsyncClientFactory {
  /**
   * Builds a regular async Java client
   *
   * @param configuration configuration
   * @return an instance of {@link S3AsyncClient}
   */
  public static S3AsyncClient createS3AsyncClient(
      @NonNull S3AsyncClientFactoryConfiguration configuration) {
    return S3AsyncClient.builder().region(configuration.getRegion()).build();
  }

  /**
   * Builds a regular async Java client
   *
   * @param configuration configuration
   * @return an instance of {@link S3AsyncClient}
   */
  public static S3AsyncClient createS3CrtAsyncClient(
      @NonNull S3AsyncClientFactoryConfiguration configuration) {
    return S3AsyncClient.crtBuilder()
        .region(configuration.getRegion())
        .minimumPartSizeInBytes(configuration.getCrtPartSizeInBytes())
        .maxNativeMemoryLimitInBytes(configuration.getCrtNativeMemoryLimitInBytes())
        .maxConcurrency(configuration.getCrtMaxConcurrency())
        .checksumValidationEnabled(configuration.isCrtChecksumValidationEnabled())
        .targetThroughputInGbps((double) configuration.getCrtTargetThroughputGbps())
        .build();
  }
}
