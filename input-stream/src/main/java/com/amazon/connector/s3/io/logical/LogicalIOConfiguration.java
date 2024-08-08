package com.amazon.connector.s3.io.logical;

import static com.amazon.connector.s3.util.Constants.ONE_MB;

import com.amazon.connector.s3.common.Configuration;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Configuration for {@link LogicalIO} */
@Getter
@Builder
@EqualsAndHashCode
public class LogicalIOConfiguration {
  private static final String PROPERTY_PREFIX = Configuration.getPrefix() + "logicalio.";

  private static final long DEFAULT_FOOTER_CACHING_SIZE = ONE_MB;
  private static final long DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD = 3 * ONE_MB;
  private static final double DEFAULT_PREDICTIVE_PREFETCHING_MIN_CONFIDENCE_RATIO = 0.3;
  private static final int DEFAULT_PARQUET_METADATA_STORE_SIZE = 45;
  private static final int DEFAULT_MAX_COLUMN_ACCESS_STORE_SIZE = 15;

  @Builder.Default private boolean footerCachingEnabled = true;

  private static final String FOOTER_CACHING_ENABLED_KEY = "footer.caching.enabled";

  @Builder.Default private long footerCachingSize = DEFAULT_FOOTER_CACHING_SIZE;

  private static final String FOOTER_CACHING_SIZE_KEY = "footer.caching.size";

  @Builder.Default private boolean smallObjectsPrefetchingEnabled = true;

  private static final String SMALL_OBJECTS_PREFETCHING_ENABLED_KEY =
      "small.objects.prefetching.enabled";

  @Builder.Default private long smallObjectSizeThreshold = DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD;

  private static final String SMALL_OBJECT_SIZE_THRESHOLD_KEY = "small.object.size.threshold";

  @Builder.Default private boolean metadataAwarePrefetchingEnabled = true;

  private static final String METADATA_AWARE_PREFETCHING_ENABLED_KEY =
      "metadata.aware.prefetching.enabled";

  @Builder.Default private boolean predictivePrefetchingEnabled = true;

  private static final String PREDICTIVE_PREFETCHING_ENABLED_KEY = "predictive.prefetching.enabled";

  // TODO: Adding temporary feature flag to control over fetching. To be removed as part of:
  // https://app.asana.com/0/1206885953994785/1207811274063025
  @Builder.Default private boolean preventOverFetchingEnabled = true;

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

  public static LogicalIOConfiguration DEFAULT = LogicalIOConfiguration.builder().build();

  /**
   * Constructs {@link LogicalIOConfiguration} from {@link Configuration} object.
   *
   * @param configuration Configuration object to generate PhysicalIOConfiguration from
   * @return LogicalIOConfiguration
   */
  public static LogicalIOConfiguration fromConfiguration(Configuration configuration) {
    return LogicalIOConfiguration.builder()
        .footerCachingEnabled(
            configuration.getBoolean(PROPERTY_PREFIX + FOOTER_CACHING_ENABLED_KEY, true))
        .footerCachingSize(
            configuration.getLong(
                PROPERTY_PREFIX + FOOTER_CACHING_SIZE_KEY, DEFAULT_FOOTER_CACHING_SIZE))
        .smallObjectsPrefetchingEnabled(
            configuration.getBoolean(PROPERTY_PREFIX + SMALL_OBJECTS_PREFETCHING_ENABLED_KEY, true))
        .smallObjectSizeThreshold(
            configuration.getLong(
                PROPERTY_PREFIX + SMALL_OBJECT_SIZE_THRESHOLD_KEY,
                DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD))
        .metadataAwarePrefetchingEnabled(
            configuration.getBoolean(
                PROPERTY_PREFIX + METADATA_AWARE_PREFETCHING_ENABLED_KEY, true))
        .predictivePrefetchingEnabled(
            configuration.getBoolean(PROPERTY_PREFIX + PREDICTIVE_PREFETCHING_ENABLED_KEY, true))
        .preventOverFetchingEnabled(
            configuration.getBoolean(PROPERTY_PREFIX + PREVENT_OVER_FETCHING_ENABLED_KEY, true))
        .parquetMetadataStoreSize(
            configuration.getInt(
                PROPERTY_PREFIX + PARQUET_METADATA_STORE_SIZE_KEY,
                DEFAULT_PARQUET_METADATA_STORE_SIZE))
        .maxColumnAccessCountStoreSize(
            configuration.getInt(
                PROPERTY_PREFIX + MAX_COLUMN_ACCESS_STORE_SIZE_KEY,
                DEFAULT_MAX_COLUMN_ACCESS_STORE_SIZE))
        .minPredictivePrefetchingConfidenceRatio(
            configuration.getDouble(
                PROPERTY_PREFIX + MIN_PREDICTIVE_PREFETCHING_CONFIDENCE_RATIO_KEY,
                DEFAULT_PREDICTIVE_PREFETCHING_MIN_CONFIDENCE_RATIO))
        .build();
  }
}
