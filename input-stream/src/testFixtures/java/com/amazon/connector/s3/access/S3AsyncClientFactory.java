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
