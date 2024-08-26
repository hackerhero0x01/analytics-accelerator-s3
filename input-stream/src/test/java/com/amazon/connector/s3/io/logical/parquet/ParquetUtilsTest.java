package com.amazon.connector.s3.io.logical.parquet;

import static com.amazon.connector.s3.util.Constants.ONE_MB;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.request.Range;
import org.junit.jupiter.api.Test;

public class ParquetUtilsTest {
  @Test
  void testGetFileTailRangeDefaultConfig() {

    Range range = ParquetUtils.getFileTailRange(LogicalIOConfiguration.DEFAULT, 0, 5 * ONE_MB);

    assertEquals(
        range.getStart(), 5 * ONE_MB - LogicalIOConfiguration.DEFAULT.getFooterCachingSize());
    assertEquals(range.getEnd(), 5 * ONE_MB - 1);
  }

  @Test
  void testGetFileTailRangeSmallFile() {

    Range range =
        ParquetUtils.getFileTailRange(
            LogicalIOConfiguration.builder()
                .smallObjectsPrefetchingEnabled(true)
                .smallObjectSizeThreshold(2 * ONE_MB)
                .build(),
            0,
            2 * ONE_MB);

    assertEquals(range.getStart(), 0);
    assertEquals(range.getEnd(), 2 * ONE_MB - 1);
  }

  @Test
  void testGetFileTailSmallContentLength() {

    Range range = ParquetUtils.getFileTailRange(LogicalIOConfiguration.DEFAULT, 0, 5);

    assertEquals(range.getStart(), 0);
    assertEquals(range.getEnd(), 4);
  }
}
