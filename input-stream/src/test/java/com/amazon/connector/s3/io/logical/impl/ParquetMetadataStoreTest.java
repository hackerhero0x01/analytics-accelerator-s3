package com.amazon.connector.s3.io.logical.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.io.logical.parquet.ColumnMetadata;
import com.amazon.connector.s3.util.S3URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ParquetMetadataStoreTest {

  @Test
  void testConstructor() {
    assertNotNull(new ParquetMetadataStore(mock(LogicalIOConfiguration.class)));
  }

  @Test
  void addRecentColumn() {
    StringBuilder concatedCoumnString =
        new StringBuilder().append("sk_test").append("sk_test_2").append("sk_test_3");
    int schemaHash = concatedCoumnString.toString().hashCode();
    Map<S3URI, ColumnMappers> columnMappersStore = new HashMap<>();

    HashMap<String, List<ColumnMetadata>> columnNameToColumnMap = new HashMap<>();

    List<ColumnMetadata> sk_testColumnMetadataList = new ArrayList<>();
    sk_testColumnMetadataList.add(new ColumnMetadata(0, "sk_test", 0, 500, schemaHash));
    columnNameToColumnMap.put("sk_test", sk_testColumnMetadataList);

    List<ColumnMetadata> sk_test_2ColumnMetadataList = new ArrayList<>();
    sk_test_2ColumnMetadataList.add(new ColumnMetadata(0, "sk_test_2", 0, 500, schemaHash));
    columnNameToColumnMap.put("sk_test_2", sk_test_2ColumnMetadataList);

    ColumnMappers columnMappers = new ColumnMappers(new HashMap<>(), columnNameToColumnMap);
    columnMappersStore.put(S3URI.of("test", "data"), columnMappers);

    Map<String, Integer> recentColumns = new HashMap<>();
    Map<Integer, Integer> maxColumnAccessCounts = new HashMap<>();
    ParquetMetadataStore parquetMetadataStore =
        new ParquetMetadataStore(
            LogicalIOConfiguration.DEFAULT,
            columnMappersStore,
            recentColumns,
            maxColumnAccessCounts);

    // First access of sk_test
    parquetMetadataStore.addRecentColumn("sk_test", S3URI.of("test", "data"));
    assertTrue(recentColumns.containsKey("sk_test"));
    assertEquals(maxColumnAccessCounts.get(schemaHash), 1);

    // sk_test is accessed again
    parquetMetadataStore.addRecentColumn("sk_test", S3URI.of("test", "data"));
    assertEquals(2, recentColumns.get("sk_test"));
    assertEquals(maxColumnAccessCounts.get(schemaHash), 2);

    parquetMetadataStore.addRecentColumn("sk_test_2", S3URI.of("test", "data"));
    parquetMetadataStore.addRecentColumn("sk_test_2", S3URI.of("test", "data"));
    parquetMetadataStore.addRecentColumn("sk_test_2", S3URI.of("test", "data"));
    // max access count for this schema should be updated with access count of sk_test_2
    assertEquals(maxColumnAccessCounts.get(schemaHash), 3);
  }
}
