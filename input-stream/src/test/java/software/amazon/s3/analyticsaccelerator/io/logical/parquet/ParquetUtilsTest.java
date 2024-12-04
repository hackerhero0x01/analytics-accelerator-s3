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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_GB;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.Range;

public class ParquetUtilsTest {
  @Test
  void testGetFileTailRangeDefaultConfig() {

    Range range =
        ParquetUtils.getFileTailRange(LogicalIOConfiguration.DEFAULT, 0, 5 * ONE_MB).get();

    assertEquals(
        range.getStart(),
        5 * ONE_MB - LogicalIOConfiguration.DEFAULT.getPrefetchFileMetadataSize());
    assertEquals(range.getEnd(), 5 * ONE_MB - 1);
  }

  @Test
  void testGetFileTailRangeSmallFile() {
    List<Range> ranges =
        ParquetUtils.getFileTailPrefetchRanges(
            LogicalIOConfiguration.builder()
                .smallObjectsPrefetchingEnabled(true)
                .smallObjectSizeThreshold(2 * ONE_MB)
                .build(),
            0,
            2 * ONE_MB);

    assertEquals(ranges.size(), 1);

    Range range = ranges.get(0);

    assertEquals(range.getStart(), 0);
    assertEquals(range.getEnd(), 2 * ONE_MB - 1);
  }

  @Test
  void testGetFileTailPrefetchRanges() {
    List<Range> ranges =
        ParquetUtils.getFileTailPrefetchRanges(LogicalIOConfiguration.DEFAULT, 0, 5 * ONE_MB);

    assertEquals(ranges.size(), 2);

    Range fileMetadataRange = ranges.get(0);
    Range pageIndexRange = ranges.get(1);

    assertEquals(
        fileMetadataRange.getStart(),
        5 * ONE_MB - LogicalIOConfiguration.DEFAULT.getPrefetchFileMetadataSize());
    assertEquals(fileMetadataRange.getEnd(), 5 * ONE_MB - 1);

    assertEquals(
        pageIndexRange.getStart(),
        5 * ONE_MB
            - LogicalIOConfiguration.DEFAULT.getPrefetchFileMetadataSize()
            - LogicalIOConfiguration.DEFAULT.getPrefetchFilePageIndexSize());
    assertEquals(
        pageIndexRange.getEnd(),
        5 * ONE_MB - LogicalIOConfiguration.DEFAULT.getPrefetchFileMetadataSize() - 1);
  }

  @Test
  void testGetLargeFileTailPrefetchRanges() {
    long contentLength = 5L * ONE_GB;

    List<Range> ranges =
        ParquetUtils.getFileTailPrefetchRanges(LogicalIOConfiguration.DEFAULT, 0, contentLength);

    assertEquals(ranges.size(), 2);

    Range fileMetadataRange = ranges.get(0);
    Range pageIndexRange = ranges.get(1);

    assertEquals(
        fileMetadataRange.getStart(),
        contentLength - LogicalIOConfiguration.DEFAULT.getPrefetchLargeFileMetadataSize());
    assertEquals(fileMetadataRange.getEnd(), contentLength - 1);

    assertEquals(
        pageIndexRange.getStart(),
        contentLength
            - LogicalIOConfiguration.DEFAULT.getPrefetchLargeFileMetadataSize()
            - LogicalIOConfiguration.DEFAULT.getPrefetchLargeFilePageIndexSize());
    assertEquals(
        pageIndexRange.getEnd(),
        contentLength - LogicalIOConfiguration.DEFAULT.getPrefetchLargeFileMetadataSize() - 1);
  }

  @Test
  void testGetFileTailSmallContentLength() {

    Range range = ParquetUtils.getFileTailRange(LogicalIOConfiguration.DEFAULT, 0, 5).get();

    assertEquals(range.getStart(), 0);
    assertEquals(range.getEnd(), 4);
  }
}
