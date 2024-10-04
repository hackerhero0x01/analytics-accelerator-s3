package com.amazon.connector.s3.benchmarks.common;

import com.amazon.connector.s3.common.ConnectorConfiguration;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/** Configuration for benchmarks */
@Value
@Builder
public class BenchmarkConfiguration {
  public static final String BUCKET_KEY = "BENCH_BUCKET";
  public static final String PREFIX_KEY = "BENCH_PREFIX";
  public static final String READ_BUFFER_SIZE_MB_KEY = "BENCH_READ_BUFFER_SIZE_MB";

  public static final int DEFAULT_READ_BUFFER_SIZE_MB_KEY = 8;

  @NonNull String bucket;
  @NonNull String prefix;
  int bufferSizeMb;
  @NonNull S3AsyncClientFactoryConfiguration clientFactoryConfiguration;

  /**
   * Creates the {@link BenchmarkConfiguration} from the supplied configuration
   *
   * @param configuration an instance of configuration
   * @return anew instance of {@link S3AsyncClientFactoryConfiguration}
   */
  public static BenchmarkConfiguration fromConfiguration(ConnectorConfiguration configuration) {
    return BenchmarkConfiguration.builder()
        .bucket(configuration.getRequiredString(BUCKET_KEY))
        .prefix(configuration.getRequiredString(PREFIX_KEY))
        .bufferSizeMb(
            configuration.getInt(READ_BUFFER_SIZE_MB_KEY, DEFAULT_READ_BUFFER_SIZE_MB_KEY))
        .clientFactoryConfiguration(
            S3AsyncClientFactoryConfiguration.fromConfiguration(configuration))
        .build();
  }

  /**
   * Creates the {@link BenchmarkConfiguration} from the environment
   *
   * @return anew instance of {@link BenchmarkConfiguration}
   */
  public static BenchmarkConfiguration fromEnvironment() {
    return fromConfiguration(new ConnectorConfiguration(System.getenv()));
  }
}
