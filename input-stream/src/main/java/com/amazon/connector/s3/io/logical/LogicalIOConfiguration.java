package com.amazon.connector.s3.io.logical;

import static com.amazon.connector.s3.util.Constants.ONE_MB;

import com.amazon.connector.s3.common.ConnectorConfiguration;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Configuration for {@link LogicalIO} */
@Getter
@Builder
@EqualsAndHashCode
public class LogicalIOConfiguration {
  private static final boolean DEFAULT_FOOTER_CACHING_ENABLED = true;
  private static final long DEFAULT_FOOTER_CACHING_SIZE = ONE_MB;
  private static final boolean DEFAULT_SMALL_OBJECT_PREFETCHING_ENABLED = true;
  private static final long DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD = 3 * ONE_MB;
  private static final boolean DEFAULT_METADATA_AWARE_PREFETCHING_ENABLED = true;
  private static final boolean DEFAULT_PREDICTIVE_PREFETCHING_ENABLED = true;
  private static final boolean DEFAULT_PREDICTIVE_PREFETCHING_ENABLED_DEFAULT = true;
  private static final double DEFAULT_PREDICTIVE_PREFETCHING_MIN_CONFIDENCE_RATIO = 0.3;
  private static final int DEFAULT_PARQUET_METADATA_STORE_SIZE = 45;
  private static final int DEFAULT_MAX_COLUMN_ACCESS_STORE_SIZE = 15;
  private static final String DEFAULT_PARQUET_FORMAT_SELECTOR_REGEX = "^.*.(parquet|par)$";

  @Builder.Default private boolean footerCachingEnabled = DEFAULT_FOOTER_CACHING_ENABLED;

  private static final String FOOTER_CACHING_ENABLED_KEY = "footer.caching.enabled";

  @Builder.Default private long footerCachingSize = DEFAULT_FOOTER_CACHING_SIZE;

  private static final String FOOTER_CACHING_SIZE_KEY = "footer.caching.size";

  @Builder.Default
  private boolean smallObjectsPrefetchingEnabled = DEFAULT_SMALL_OBJECT_PREFETCHING_ENABLED;

  private static final String SMALL_OBJECTS_PREFETCHING_ENABLED_KEY =
      "small.objects.prefetching.enabled";

  @Builder.Default private long smallObjectSizeThreshold = DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD;

  private static final String SMALL_OBJECT_SIZE_THRESHOLD_KEY = "small.object.size.threshold";

  @Builder.Default
  private boolean metadataAwarePrefetchingEnabled = DEFAULT_METADATA_AWARE_PREFETCHING_ENABLED;

  private static final String METADATA_AWARE_PREFETCHING_ENABLED_KEY =
      "metadata.aware.prefetching.enabled";

  @Builder.Default
  private boolean predictivePrefetchingEnabled = DEFAULT_PREDICTIVE_PREFETCHING_ENABLED;

  private static final String PREDICTIVE_PREFETCHING_ENABLED_KEY = "predictive.prefetching.enabled";

  // TODO: Adding temporary feature flag to control over fetching. To be removed as part of:
  // https://app.asana.com/0/1206885953994785/1207811274063025
  @Builder.Default
  private boolean preventOverFetchingEnabled = DEFAULT_PREDICTIVE_PREFETCHING_ENABLED_DEFAULT;

  private static final String PREVENT_OVER_FETCHING_ENABLED_KEY = "prevent.over.fetching.enabled";

  @Builder.Default private int parquetMetadataStoreSize = DEFAULT_PARQUET_METADATA_STORE_SIZE;

  private static final String PARQUET_METADATA_STORE_SIZE_KEY = "parquet.metadata.store.size";

  @Builder.Default private int maxColumnAccessCountStoreSize = DEFAULT_MAX_COLUMN_ACCESS_STORE_SIZE;

  private static final String MAX_COLUMN_ACCESS_STORE_SIZE_KEY = "max.column.access.store.size";

  @Builder.Default
  private double minPredictivePrefetchingConfidenceRatio =
      DEFAULT_PREDICTIVE_PREFETCHING_MIN_CONFIDENCE_RATIO;

  private static final String MIN_PREDICTIVE_PREFETCHING_CONFIDENCE_RATIO_KEY =
      "min.predictive.prefetching.confidence.ratio";

  @Builder.Default
  private String parquetFormatSelectorRegex = DEFAULT_PARQUET_FORMAT_SELECTOR_REGEX;

  private static final String PARQUET_FORMAT_SELECTOR_REGEX = "parquet.format.selector.regex";

  public static final LogicalIOConfiguration DEFAULT = LogicalIOConfiguration.builder().build();

  /**
   * Constructs {@link LogicalIOConfiguration} from {@link ConnectorConfiguration} object.
   *
   * @param configuration Configuration object to generate PhysicalIOConfiguration from
   * @return LogicalIOConfiguration
   */
  public static LogicalIOConfiguration fromConfiguration(ConnectorConfiguration configuration) {
    return LogicalIOConfiguration.builder()
        .footerCachingEnabled(
            configuration.getBoolean(FOOTER_CACHING_ENABLED_KEY, DEFAULT_FOOTER_CACHING_ENABLED))
        .footerCachingSize(
            configuration.getLong(FOOTER_CACHING_SIZE_KEY, DEFAULT_FOOTER_CACHING_SIZE))
        .smallObjectsPrefetchingEnabled(
            configuration.getBoolean(
                SMALL_OBJECTS_PREFETCHING_ENABLED_KEY, DEFAULT_SMALL_OBJECT_PREFETCHING_ENABLED))
        .smallObjectSizeThreshold(
            configuration.getLong(
                SMALL_OBJECT_SIZE_THRESHOLD_KEY, DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD))
        .metadataAwarePrefetchingEnabled(
            configuration.getBoolean(
                METADATA_AWARE_PREFETCHING_ENABLED_KEY, DEFAULT_METADATA_AWARE_PREFETCHING_ENABLED))
        .predictivePrefetchingEnabled(
            configuration.getBoolean(
                PREDICTIVE_PREFETCHING_ENABLED_KEY, DEFAULT_PREDICTIVE_PREFETCHING_ENABLED))
        .preventOverFetchingEnabled(
            configuration.getBoolean(
                PREVENT_OVER_FETCHING_ENABLED_KEY, DEFAULT_PREDICTIVE_PREFETCHING_ENABLED_DEFAULT))
        .parquetMetadataStoreSize(
            configuration.getInt(
                PARQUET_METADATA_STORE_SIZE_KEY, DEFAULT_PARQUET_METADATA_STORE_SIZE))
        .maxColumnAccessCountStoreSize(
            configuration.getInt(
                MAX_COLUMN_ACCESS_STORE_SIZE_KEY, DEFAULT_MAX_COLUMN_ACCESS_STORE_SIZE))
        .minPredictivePrefetchingConfidenceRatio(
            configuration.getDouble(
                MIN_PREDICTIVE_PREFETCHING_CONFIDENCE_RATIO_KEY,
                DEFAULT_PREDICTIVE_PREFETCHING_MIN_CONFIDENCE_RATIO))
        .parquetFormatSelectorRegex(
            configuration.getString(
                PARQUET_FORMAT_SELECTOR_REGEX, DEFAULT_PARQUET_FORMAT_SELECTOR_REGEX))
        .build();
  }
}
