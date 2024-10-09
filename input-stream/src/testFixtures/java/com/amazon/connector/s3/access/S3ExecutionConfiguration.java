package com.amazon.connector.s3.access;

import static com.amazon.connector.s3.access.SizeConstants.ONE_MB_IN_BYTES;

import com.amazon.connector.s3.common.ConnectorConfiguration;
import com.amazon.connector.s3.util.S3URI;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/** Configuration for benchmarks */
@Value
@Builder
public class S3ExecutionConfiguration {
  public static final String BUCKET_KEY = "S3_TEST_BUCKET";
  public static final String PREFIX_KEY = "S3_TEST_PREFIX";
  public static final String READ_BUFFER_SIZE_MB_KEY = "S3_TEST_READ_BUFFER_SIZE_MB";
  public static final int DEFAULT_READ_BUFFER_SIZE_MB_KEY = 8;

  @NonNull String bucket;
  @NonNull String prefix;
  int bufferSizeMb;
  @NonNull S3AsyncClientFactoryConfiguration clientFactoryConfiguration;

  /**
   * Creates the {@link S3ExecutionConfiguration} from the supplied configuration
   *
   * @param configuration an instance of configuration
   * @return anew instance of {@link S3AsyncClientFactoryConfiguration}
   */
  public static S3ExecutionConfiguration fromConfiguration(ConnectorConfiguration configuration) {
    return S3ExecutionConfiguration.builder()
        .bucket(configuration.getRequiredString(BUCKET_KEY))
        .prefix(configuration.getRequiredString(PREFIX_KEY))
        .bufferSizeMb(
            configuration.getInt(READ_BUFFER_SIZE_MB_KEY, DEFAULT_READ_BUFFER_SIZE_MB_KEY))
        .clientFactoryConfiguration(
            S3AsyncClientFactoryConfiguration.fromConfiguration(configuration))
        .build();
  }

  /**
   * Returns the base Uri
   *
   * @return base Uri
   */
  public S3URI getBaseUri() {
    return S3URI.of(this.getBucket(), this.getPrefix());
  }

  /**
   * Returns the buffer size in bytes
   *
   * @return buffer size in bytes
   */
  public int getBufferSizeBytes() {
    return ONE_MB_IN_BYTES * this.getBufferSizeMb();
  }

  /**
   * Creates the {@link S3ExecutionConfiguration} from the environment
   *
   * @return anew instance of {@link S3ExecutionConfiguration}
   */
  public static S3ExecutionConfiguration fromEnvironment() {
    return fromConfiguration(new ConnectorConfiguration(System.getenv()));
  }
}
