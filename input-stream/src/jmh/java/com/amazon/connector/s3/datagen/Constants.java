package com.amazon.connector.s3.datagen;

/** Constants related to microbenchmark data generation */
public class Constants {
  public static final int ONE_KB_IN_BYTES = 1024;
  public static final int ONE_MB_IN_BYTES = 1024 * ONE_KB_IN_BYTES;
  public static final String BENCHMARK_BUCKET = "gccsenge-microbenchmarks-dub";
  public static final String BENCHMARK_DATA_PREFIX_SEQUENTIAL =
      "s3-connector-framework-benchmark/sequential/";
}
