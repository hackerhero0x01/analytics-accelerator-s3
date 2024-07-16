package com.amazon.connector.s3.io.logical;

import static com.amazon.connector.s3.util.Constants.DEFAULT_FOOTER_CACHING_SIZE;
import static com.amazon.connector.s3.util.Constants.DEFAULT_MAX_COLUMN_ACCESS_STORE_SIZE;
import static com.amazon.connector.s3.util.Constants.DEFAULT_PARQUET_METADATA_PROCESSING_TIMEOUT_MS;
import static com.amazon.connector.s3.util.Constants.DEFAULT_PARQUET_METADATA_SIZE_LIMIT_BYTES;
import static com.amazon.connector.s3.util.Constants.DEFAULT_PARQUET_METADATA_STORE_SIZE;
import static com.amazon.connector.s3.util.Constants.DEFAULT_PARQUET_PARSING_POOL_SIZE;
import static com.amazon.connector.s3.util.Constants.DEFAULT_PREDICTIVE_PREFETCHING_MIN_CONFIDENCE_RATIO;
import static com.amazon.connector.s3.util.Constants.DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Configuration for {@link LogicalIO} */
@Getter
@Builder
@EqualsAndHashCode
public class LogicalIOConfiguration {
  @Builder.Default private boolean footerCachingEnabled = true;

  @Builder.Default private long footerCachingSize = DEFAULT_FOOTER_CACHING_SIZE;

  @Builder.Default private boolean smallObjectsPrefetchingEnabled = true;

  @Builder.Default private long smallObjectSizeThreshold = DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD;

  @Builder.Default private boolean metadataAwarePrefetchingEnabled = true;

  @Builder.Default private boolean predictivePrefetchingEnabled = true;

  // TODO: Adding temporary feature flag to control over fetching. To be removed as part of:
  // https://app.asana.com/0/1206885953994785/1207811274063025
  @Builder.Default private boolean preventOverFetchingEnabled = true;

  @Builder.Default private int parquetMetadataStoreSize = DEFAULT_PARQUET_METADATA_STORE_SIZE;

  @Builder.Default private int maxColumnAccessCountStoreSize = DEFAULT_MAX_COLUMN_ACCESS_STORE_SIZE;

  @Builder.Default
  private long parquetMetadataProcessingTimeoutMs = DEFAULT_PARQUET_METADATA_PROCESSING_TIMEOUT_MS;

  @Builder.Default private int parquetParsingPoolSize = DEFAULT_PARQUET_PARSING_POOL_SIZE;

  @Builder.Default
  private long parquetMetadataSizeLimit = DEFAULT_PARQUET_METADATA_SIZE_LIMIT_BYTES;

  @Builder.Default
  private double minPredictivePrefetchingConfidenceRatio =
      DEFAULT_PREDICTIVE_PREFETCHING_MIN_CONFIDENCE_RATIO;

  public static LogicalIOConfiguration DEFAULT = LogicalIOConfiguration.builder().build();
}
