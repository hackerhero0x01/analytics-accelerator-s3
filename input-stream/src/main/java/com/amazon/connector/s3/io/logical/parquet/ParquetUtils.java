package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.request.Range;

import java.util.ArrayList;
import java.util.List;
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

// Constructs a list of row groups to prefetch.
// The idea is that if you have information about the current column being read, then prefetching
  // is in "cautious" mode, and you're only prefetching on the READ. in this case,
// only prefetch columns for the current row group. If prefetching is in the default mode, then
  // prefetching is happening on open file! Prefetch the first row group, in the future this can be
  // extended to prefetch n + 1 row groups (so prefetch 0, 1, 2 row groups). 
  public static List<Integer> constructRowGroupsToPrefetch(
      Optional<ColumnMetadata> columnMetadataOptional) {

    List<Integer> rowGroupsToPrefetch = new ArrayList<>();

    if (columnMetadataOptional.isPresent()) {
      ColumnMetadata columnMetadata = columnMetadataOptional.get();
      rowGroupsToPrefetch.add(columnMetadata.getRowGroupIndex());
    } else {
      rowGroupsToPrefetch.add(0);
    }

    return rowGroupsToPrefetch;
  }
}
