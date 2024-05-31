package com.amazon.connector.s3.io.logical;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import static com.amazon.connector.s3.util.Constants.ONE_MB;

/** Configuration for {@link LogicalIO} */
@Getter
@Builder
@EqualsAndHashCode
public class LogicalIOConfiguration {
    public static final long DEFAULT_FOOTER_PRECACHING_SIZE = ONE_MB;
    public static final long DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD = 3 * ONE_MB;

    @Builder.Default private boolean FooterPrecachingEnabled = false;

    @Builder.Default private long FooterPrecachingSize = DEFAULT_FOOTER_PRECACHING_SIZE;

    @Builder.Default private boolean SmallObjectsPrefetchingEnabled = false;

    @Builder.Default private long SmallObjectSizeThreshold = DEFAULT_SMALL_OBJECT_SIZE_THRESHOLD;

    public static LogicalIOConfiguration DEFAULT = LogicalIOConfiguration.builder().build();
}
