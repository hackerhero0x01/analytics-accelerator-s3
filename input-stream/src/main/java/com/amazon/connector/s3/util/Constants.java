package com.amazon.connector.s3.util;

/** S3SeekableInputStream constants. */
public class Constants {

  public static final int ONE_KB = 1024;
  public static final int ONE_MB = 1024 * 1024;
  public static final int PARQUET_MAGIC_STR_LENGTH = 4;
  public static final int PARQUET_FOOTER_LENGTH_SIZE = 4;

  public static final int DEFAULT_PARQUET_METADATA_STORE_SIZE = 45;

  public static final int DEFAULT_MAX_COLUMN_ACCESS_STORE_SIZE = 15;

  public static final long DEFAULT_FOOTER_CACHING_SIZE = 64 * ONE_KB;
  public static final long DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD = 3 * ONE_MB;
  public static final double DEFAULT_PREDICTIVE_PREFETCHING_MIN_CONFIDENCE_RATIO = 0.3;
}
