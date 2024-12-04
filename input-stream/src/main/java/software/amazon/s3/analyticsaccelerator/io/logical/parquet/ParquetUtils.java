/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.io.logical.parquet;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.PrefetchMode;

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

    FooterPrefetchSize footerPrefetchSize =
        getFooterPrefetchSize(logicalIOConfiguration, contentLength);

    if (contentLength > footerPrefetchSize.getSize()) {
      startRange = contentLength - footerPrefetchSize.getFileMetadataPrefetchSize();
    }

    // Return a range if we have non-zero range to work with, and Empty otherwise
    if (startRange < contentLength) {
      return Optional.of(new Range(startRange, contentLength - 1));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Gets the ranges to prefetch from the tail. If the file is < smallObject threshold, then
   * prefetch the whole file. Else, prefetch the fileMetadata and the pageIndex structures as
   * separate requests. The fileMetadata will always be required, pageIndex may be required
   * depending on the engine being used.
   *
   * @param logicalIOConfiguration logical io configuration
   * @param startRange start of file
   * @param contentLength length of file
   * @return List of prefetch requests to make
   */
  public static List<Range> getFileTailPrefetchRanges(
      LogicalIOConfiguration logicalIOConfiguration, long startRange, long contentLength) {

    List<Range> ranges = new ArrayList<>();

    FooterPrefetchSize footerPrefetchSize =
        getFooterPrefetchSize(logicalIOConfiguration, contentLength);

    if (contentLength > footerPrefetchSize.getSize()) {

      boolean shouldPrefetchSmallFile =
          logicalIOConfiguration.isSmallObjectsPrefetchingEnabled()
              && contentLength <= logicalIOConfiguration.getSmallObjectSizeThreshold();

      if (!shouldPrefetchSmallFile) {
        long fileMetadataStartIndex =
            contentLength - footerPrefetchSize.getFileMetadataPrefetchSize();
        ranges.add(new Range(fileMetadataStartIndex, contentLength - 1));

        if (logicalIOConfiguration.isPrefetchPageIndexEnabled()) {
          ranges.add(
              new Range(
                  fileMetadataStartIndex - footerPrefetchSize.getPageIndexPrefetchSize(),
                  fileMetadataStartIndex - 1));
        }

        return ranges;
      }
    }

    if (startRange < contentLength) {
      ranges.add(new Range(startRange, contentLength - 1));
    }

    return ranges;
  }

  private static FooterPrefetchSize getFooterPrefetchSize(
      LogicalIOConfiguration logicalIOConfiguration, long contentLength) {
    if (contentLength > logicalIOConfiguration.getLargeFileSize()) {
      return new FooterPrefetchSize(
          logicalIOConfiguration.getPrefetchLargeFileMetadataSize(),
          logicalIOConfiguration.getPrefetchLargeFilePageIndexSize());
    } else {
      return new FooterPrefetchSize(
          logicalIOConfiguration.getPrefetchFileMetadataSize(),
          logicalIOConfiguration.getPrefetchFilePageIndexSize());
    }
  }

  /**
   * Constructs a list of row groups to prefetch. Used when {@link PrefetchMode} is equal to ALL. In
   * this mode, prefetching of recent columns happens on the first open of the Parquet file. For
   * this, only prefetch columns from the first row group.
   *
   * @return List<Integer> List of row group indexes to prefetch
   */
  public static List<Integer> constructRowGroupsToPrefetch() {
    List<Integer> rowGroupsToPrefetch = new ArrayList<>();
    rowGroupsToPrefetch.add(0);
    return rowGroupsToPrefetch;
  }

  /**
   * Constructs a list of row groups to prefetch. Used when {@link PrefetchMode} is equal to
   * ROW_GROUP. In this mode, prefetching of recent columns happens only when a read to a column of
   * the currently open Parquet file is detected. For this, only prefetch columns from the row group
   * to which this column belongs.
   *
   * @param columnMetadata Column metadata of the current column being read
   * @return List<Integer> List of row group indexes to prefetch
   */
  public static List<Integer> constructRowGroupsToPrefetch(ColumnMetadata columnMetadata) {

    List<Integer> rowGroupsToPrefetch = new ArrayList<>();
    rowGroupsToPrefetch.add(columnMetadata.getRowGroupIndex());

    return rowGroupsToPrefetch;
  }
}
