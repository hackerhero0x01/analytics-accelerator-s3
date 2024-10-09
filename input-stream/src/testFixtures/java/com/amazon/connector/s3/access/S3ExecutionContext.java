package com.amazon.connector.s3.access;

import java.io.Closeable;
import java.io.IOException;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

/** This carries the state of the benchmark execution */
@Getter
public class S3ExecutionContext implements Closeable {
  @NonNull private final S3ExecutionConfiguration configuration;
  @NonNull private final S3AsyncClient s3Client;
  @NonNull private final S3AsyncClient s3CrtClient;

  /**
   * Creates an instance of {@link S3ExecutionContext}
   *
   * @param configuration an instance of {@link S3ExecutionConfiguration}
   */
  public S3ExecutionContext(@NonNull S3ExecutionConfiguration configuration) {
    this.configuration = configuration;
    this.s3Client =
        S3AsyncClientFactory.createS3AsyncClient(configuration.getClientFactoryConfiguration());
    this.s3CrtClient =
        S3AsyncClientFactory.createS3CrtAsyncClient(configuration.getClientFactoryConfiguration());

    // test connections
    testConnection(this.s3Client, configuration);
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
