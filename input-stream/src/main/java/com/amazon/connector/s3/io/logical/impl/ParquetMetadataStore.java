package com.amazon.connector.s3.io.logical.impl;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.io.logical.parquet.ColumnMetadata;
import com.amazon.connector.s3.util.S3URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;

/** Object to aggregate column usage statistics from Parquet files */
public class ParquetMetadataStore {

  private final LogicalIOConfiguration configuration;

  private final Map<S3URI, ColumnMappers> columnMappersStore;

  private final Map<String, Integer> recentColumns;

  @Getter private final Map<Integer, Integer> maxColumnAccessCounts;

  /**
   * Creates a new instance of ParquetMetadataStore.
   *
   * @param configuration object containing information about the metadata store size
   */
  public ParquetMetadataStore(LogicalIOConfiguration configuration) {
    this(
        configuration,
        Collections.synchronizedMap(
            new LinkedHashMap<S3URI, ColumnMappers>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry eldest) {
                return this.size() > configuration.getParquetMetadataStoreSize();
              }
            }),
        Collections.synchronizedMap(
            new LinkedHashMap<String, Integer>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry eldest) {
                return this.size() > configuration.getParquetMetadataStoreSize();
              }
            }),
        Collections.synchronizedMap(
            new LinkedHashMap<Integer, Integer>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry eldest) {
                return this.size() > configuration.getMaxColumnAccessCountStoreSize();
              }
            }));
  }

  /**
   * Creates a new instance of ParquetMetadataStore. This constructor is used for dependency
   * injection.
   *
   * @param logicalIOConfiguration The logical IO configuration
   * @param columnMappersStore Store of column mappings
   * @param recentColumns Recent columns being read
   * @param maxColumnAccessCounts The maximum access count of a column per schema
   */
  protected ParquetMetadataStore(
      LogicalIOConfiguration logicalIOConfiguration,
      Map<S3URI, ColumnMappers> columnMappersStore,
      Map<String, Integer> recentColumns,
      Map<Integer, Integer> maxColumnAccessCounts) {
    this.configuration = logicalIOConfiguration;
    this.columnMappersStore = columnMappersStore;
    this.recentColumns = recentColumns;
    this.maxColumnAccessCounts = maxColumnAccessCounts;
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
   * @param s3URI s3URI of current object being read
   */
  public void addRecentColumn(String columnName, S3URI s3URI) {
    int columnAccessCount = recentColumns.getOrDefault(columnName, 0) + 1;
    recentColumns.put(columnName, columnAccessCount);
    updateMaxColumnAccessCounts(columnAccessCount, s3URI, columnName);
  }

  /**
   * Gets a list of recent columns being read.
   *
   * @return Set of recent columns being
   */
  public Set<Map.Entry<String, Integer>> getRecentColumns() {
    return recentColumns.entrySet();
  }

  private void updateMaxColumnAccessCounts(int columnAccessCount, S3URI s3URI, String columnName) {
    if (columnMappersStore.containsKey(s3URI)) {
      List<ColumnMetadata> columnMetadataList =
          columnMappersStore.get(s3URI).getColumnNameToColumnMap().get(columnName);
      // Update the max access of a column for a particular schema.
      if (!columnMetadataList.isEmpty()) {
        int schemaHash = columnMetadataList.get(0).getSchemaHash();
        int currMaxColumnAccessCount = maxColumnAccessCounts.getOrDefault(schemaHash, 0);
        maxColumnAccessCounts.put(
            schemaHash, Math.max(columnAccessCount, currMaxColumnAccessCount));
      }
    }
  }
}
