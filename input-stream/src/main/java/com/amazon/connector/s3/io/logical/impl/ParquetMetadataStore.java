package com.amazon.connector.s3.io.logical.impl;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.util.S3URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import lombok.Getter;

/** Object to aggregate column usage statistics from Parquet files */
public class ParquetMetadataStore {

  private final LogicalIOConfiguration configuration;

  private final Map<S3URI, ColumnMappers> columnMappersStore;

  private final Map<String, Integer> recentColumns;

  @Getter private int maxColumnAccessCount = 1;

  /**
   * Creates a new instance of ParquetMetadataStore.
   *
   * @param configuration object containing information about the metadata store size
   */
  public ParquetMetadataStore(LogicalIOConfiguration configuration) {
    this.configuration = configuration;

    this.columnMappersStore =
        Collections.synchronizedMap(
            new LinkedHashMap<S3URI, ColumnMappers>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry eldest) {
                return this.size() > configuration.getParquetMetadataStoreSize();
              }
            });

    this.recentColumns =
        Collections.synchronizedMap(
            new LinkedHashMap<String, Integer>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry eldest) {
                return this.size() > configuration.getParquetMetadataStoreSize();
              }
            });
  }

  /**
   * Gets column mappers for a key.
   *
   * @param s3URI The S3URI to get column mappers for.
   * @return Column mappings
   */
  public ColumnMappers getColumnMappers(S3URI s3URI) {
    return columnMappersStore.get(s3URI);
  }

  /**
   * Stores column mappers for an object.
   *
   * @param s3URI S3URI to store mappers for
   * @param columnMappers Parquet metadata column mappings
   */
  public void putColumnMappers(S3URI s3URI, ColumnMappers columnMappers) {
    columnMappersStore.put(s3URI, columnMappers);
  }

  /**
   * Adds column to list of recent columns.
   *
   * @param columnName column to be added
   */
  public void addRecentColumn(String columnName) {
    int columnAccessCount = recentColumns.getOrDefault(columnName, 0) + 1;
    recentColumns.put(columnName, columnAccessCount);
    maxColumnAccessCount = Math.max(maxColumnAccessCount, columnAccessCount);
  }

  /**
   * Gets a list of recent columns being read.
   *
   * @return Set of recent columns being
   */
  public Set<Map.Entry<String, Integer>> getRecentColumns() {
    return recentColumns.entrySet();
  }
}
