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
package software.amazon.s3.analyticsaccelerator.access;

import java.io.Closeable;
import java.io.IOException;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;

/** This carries the state of the benchmark execution */
@Getter
public class S3ExecutionContext implements Closeable {
  @NonNull private final S3ExecutionConfiguration configuration;
  @NonNull private final S3AsyncClient s3AsyncClient;
  @NonNull private final S3Client s3Client;
  @NonNull private final S3AsyncClient s3CrtClient;
  @NonNull private final S3AsyncClient faultyS3Client;

  /**
   * Creates an instance of {@link S3ExecutionContext}
   *
   * @param configuration an instance of {@link S3ExecutionConfiguration}
   */
  public S3ExecutionContext(@NonNull S3ExecutionConfiguration configuration) {
    this.configuration = configuration;
    this.s3AsyncClient =
        S3ClientFactory.createS3AsyncClient(configuration.getClientFactoryConfiguration());
    this.s3CrtClient =
        S3ClientFactory.createS3CrtAsyncClient(configuration.getClientFactoryConfiguration());
    this.faultyS3Client =
        S3ClientFactory.createFaultyS3Client(configuration.getClientFactoryConfiguration());
    this.s3Client = S3ClientFactory.createS3Client(configuration.getClientFactoryConfiguration());

    // test connections
    testConnection(this.s3AsyncClient, configuration);
    testConnection(this.s3CrtClient, configuration);
  }

  /**
   * Test connection by issuing a list against the bucket and prefix
   *
   * @param s3Client the client
   * @param configuration configuration
   */
  private static void testConnection(
      S3AsyncClient s3Client, S3ExecutionConfiguration configuration) {
    ListObjectsV2Request listObjectsV2Request =
        ListObjectsV2Request.builder()
            .bucket(configuration.getBucket())
            .prefix(configuration.getPrefix())
            .maxKeys(10)
            .build();
    s3Client.listObjectsV2(listObjectsV2Request).join();
  }

  /**
   * Closes all the resources associated with the context
   *
   * @throws IOException any IO error thrown
   */
  @Override
  public void close() throws IOException {
    this.s3Client.close();
    this.s3CrtClient.close();
  }
}
