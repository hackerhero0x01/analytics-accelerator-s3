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

  /**
   * Constructs a list of row groups to prefetch. Used when {@link
   * com.amazon.connector.s3.util.PrefetchMode} is equal to ALL. In this mode, prefetching of recent
   * columns happens on the first open of the Parquet file. For this, only prefetch columns from the
   * first row group.
   *
   * @return List<Integer> List of row group indexes to prefetch
   */
  public static List<Integer> constructRowGroupsToPrefetch() {
    List<Integer> rowGroupsToPrefetch = new ArrayList<>();
    rowGroupsToPrefetch.add(0);
    return rowGroupsToPrefetch;
  }

  /**
   * Constructs a list of row groups to prefetch. Used when {@link
   * com.amazon.connector.s3.util.PrefetchMode} is equal to ROW_GROUP. In this mode, prefetching of
   * recent columns happens only when a read to a column of the currently open Parquet file is
   * detected. For this, only prefetch columns from the row group to which this column belongs.
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
