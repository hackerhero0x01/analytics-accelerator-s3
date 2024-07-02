package com.amazon.connector.s3.io.logical;

import static com.amazon.connector.s3.util.Constants.DEFAULT_FOOTER_CACHING_SIZE;
import static com.amazon.connector.s3.util.Constants.DEFAULT_PARQUET_METADATA_STORE_SIZE;
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

  @Builder.Default private int parquetMetadataStoreSize = DEFAULT_PARQUET_METADATA_STORE_SIZE;

  public static LogicalIOConfiguration DEFAULT = LogicalIOConfiguration.builder().build();
}
