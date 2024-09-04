package com.amazon.connector.s3.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ObjectFormatSelectorTest {

  @ParameterizedTest
  @ValueSource(strings = {"key.parquet", "key.par"})
  public void testDefaultConfigParquetLogicalIOSelection(String key) {
    ObjectFormatSelector objectFormatSelector =
        new ObjectFormatSelector(LogicalIOConfiguration.DEFAULT);

    assertEquals(
        objectFormatSelector.getObjectFormat(S3URI.of("bucket", key)), ObjectFormat.PARQUET);
  }

  @ParameterizedTest
  @ValueSource(strings = {"key.pr3", "key.par3"})
  public void testConfiguredExtensionParquetLogicalIOSelection(String key) {
    // Build with configuration that accepts ".pr3" and "par3" are parquet file extensions.
    ObjectFormatSelector objectFormatSelector =
        new ObjectFormatSelector(
            LogicalIOConfiguration.builder().parquetFormatSelectorRegex("^.*.(pr3|par3)$").build());

    assertEquals(
        objectFormatSelector.getObjectFormat(S3URI.of("bucket", key)), ObjectFormat.PARQUET);
  }

  @ParameterizedTest
  @ValueSource(strings = {"key.jar", "key.txt", "key.parque", "key.pa"})
  public void testNonParquetLogicalIOSelection(String key) {
    ObjectFormatSelector objectFormatSelector =
        new ObjectFormatSelector(LogicalIOConfiguration.DEFAULT);

    assertEquals(
        objectFormatSelector.getObjectFormat(S3URI.of("bucket", key)), ObjectFormat.DEFAULT);
  }
}
