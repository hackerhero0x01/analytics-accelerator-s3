package com.amazon.connector.s3.io.logical.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.io.logical.parquet.ColumnMetadata;
import com.amazon.connector.s3.util.S3URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class ParquetColumnPrefetchStoreTest {

  @Test
  void testConstructor() {
    assertNotNull(new ParquetColumnPrefetchStore(mock(LogicalIOConfiguration.class)));
  }

  @Test
  void addRecentColumn() {
    StringBuilder concatedColumnString =
        new StringBuilder().append("sk_test").append("sk_test_2").append("sk_test_3");
    int schemaHash = concatedColumnString.toString().hashCode();
    Map<S3URI, ColumnMappers> columnMappersStore = new HashMap<>();

    ColumnMetadata sk_test = new ColumnMetadata(0, "sk_test", 0, 500, schemaHash);
    ColumnMetadata sk_test2 = new ColumnMetadata(0, "sk_test2", 0, 500, schemaHash);
    ColumnMetadata sk_test3 = new ColumnMetadata(0, "sk_test3", 0, 500, schemaHash);

    Map<Integer, LinkedList<String>> recentlyReadColumnsPerSchema = new HashMap<>();
    ParquetColumnPrefetchStore parquetColumnPrefetchStore =
        new ParquetColumnPrefetchStore(
            LogicalIOConfiguration.builder().maxColumnAccessCountStoreSize(3).build(),
            columnMappersStore,
            recentlyReadColumnsPerSchema);

    parquetColumnPrefetchStore.addRecentColumn(sk_test);
    parquetColumnPrefetchStore.addRecentColumn(sk_test2);
    parquetColumnPrefetchStore.addRecentColumn(sk_test);
    parquetColumnPrefetchStore.addRecentColumn(sk_test2);
    parquetColumnPrefetchStore.addRecentColumn(sk_test3);
    parquetColumnPrefetchStore.addRecentColumn(sk_test3);

    assertEquals(recentlyReadColumnsPerSchema.get(schemaHash).size(), 3);

    // We should only have the last 3 recently read columns in the list
    List<String> expectedColumns = new ArrayList<>();
    expectedColumns.add("sk_test2");
    expectedColumns.add("sk_test3");
    expectedColumns.add("sk_test3");

    assertEquals(recentlyReadColumnsPerSchema.get(schemaHash), expectedColumns);

    // We should only have unique columns in the list
    Set<String> expectedUniqueColumns = new HashSet<>();
    expectedUniqueColumns.add("sk_test2");
    expectedUniqueColumns.add("sk_test3");

    assertEquals(
        parquetColumnPrefetchStore.getUniqueRecentColumnsForSchema(schemaHash),
        expectedUniqueColumns);
  }
}
