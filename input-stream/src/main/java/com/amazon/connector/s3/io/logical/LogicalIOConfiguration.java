package com.amazon.connector.s3.io.logical;

import static com.amazon.connector.s3.util.Constants.DEFAULT_FOOTER_PRECACHING_SIZE;
import static com.amazon.connector.s3.util.Constants.DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Configuration for {@link LogicalIO} */
@Getter
@Builder
@EqualsAndHashCode
public class LogicalIOConfiguration {
  @Builder.Default private boolean FooterPrecachingEnabled = true;

  @Builder.Default private long FooterPrecachingSize = DEFAULT_FOOTER_PRECACHING_SIZE;

  @Builder.Default private boolean SmallObjectsPrefetchingEnabled = true;

  @Builder.Default private long SmallObjectSizeThreshold = DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD;

  public static LogicalIOConfiguration DEFAULT = LogicalIOConfiguration.builder().build();
}
