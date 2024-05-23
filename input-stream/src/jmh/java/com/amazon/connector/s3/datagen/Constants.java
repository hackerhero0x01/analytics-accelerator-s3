package com.amazon.connector.s3.datagen;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;

/** Constants related to microbenchmark data generation */
public class Constants {

  public static final String BENCHMARK_BUCKET;
  private static final String BENCHMARK_BUCKET_ENV_VAR = "BENCHMARK_BUCKET";

  public static final String BENCHMARK_DATA_PREFIX;
  private static final String BENCHMARK_DATA_PREFIX_ENV_VAR = "BENCHMARK_DATA_PREFIX";

  static {
    BENCHMARK_BUCKET = System.getenv(BENCHMARK_BUCKET_ENV_VAR);
    BENCHMARK_DATA_PREFIX = System.getenv(BENCHMARK_DATA_PREFIX_ENV_VAR);

    // Sanity check setup (credentials + does the bucket exist) by sending a HeadBucket to the
    // bucket
    try {
      S3AsyncClient client = S3AsyncClient.create();
      client.headBucket(HeadBucketRequest.builder().bucket(BENCHMARK_BUCKET).build()).join();
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Could not initialise micro-benchmarks because the environment setup is likely incorrect. We could not "
                  + "do a HeadBucket against your bucket. Please ensure you correctly populate environment variables %s and "
                  + "%s before running ./gradlew jmh",
              BENCHMARK_BUCKET_ENV_VAR, BENCHMARK_DATA_PREFIX_ENV_VAR),
          e);
    }
  }

  public static final int ONE_KB_IN_BYTES = 1024;
  public static final int ONE_MB_IN_BYTES = 1024 * ONE_KB_IN_BYTES;
  public static final long ONE_GB_IN_BYTES = 1024 * ONE_MB_IN_BYTES;

  public static final String BENCHMARK_DATA_PREFIX_SEQUENTIAL =
      BENCHMARK_DATA_PREFIX + "/sequential/";
}
