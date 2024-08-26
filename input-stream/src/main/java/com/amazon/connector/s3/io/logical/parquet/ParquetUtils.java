package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.request.Range;
import java.util.Optional;

/** Utils class for the Parquet logical layer. */
public final class ParquetUtils {
  /** Prevent direct instantiation, this is meant to be a facade. */
  private ParquetUtils() {}
  /**
   * Gets range of file tail to be read.
   *
   * @param logicalIOConfiguration logical io configuration
   * @param startRange start of file
   * @param contentLength length of file
   * @return range to be read
   */
  public static Optional<Range> getFileTailRange(
      LogicalIOConfiguration logicalIOConfiguration, long startRange, long contentLength) {

    if (contentLength > logicalIOConfiguration.getFooterCachingSize()) {
      boolean shouldPrefetchSmallFile =
          logicalIOConfiguration.isSmallObjectsPrefetchingEnabled()
              && contentLength <= logicalIOConfiguration.getSmallObjectSizeThreshold();

      if (!shouldPrefetchSmallFile) {
        startRange = contentLength - logicalIOConfiguration.getFooterCachingSize();
      }
    }

    // Return a range if we have non-zero range to work with, and Empty otherwise
    if (startRange < contentLength) {
      return Optional.of(new Range(startRange, contentLength - 1));
    } else {
      return Optional.empty();
    }
  }
}
