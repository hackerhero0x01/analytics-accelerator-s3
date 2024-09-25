package com.amazon.connector.s3.io.logical.impl;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.io.logical.parquet.ColumnMetadata;
import com.amazon.connector.s3.util.S3URI;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Object to aggregate column usage statistics from Parquet files */
@SuppressFBWarnings(
    value = "SE_BAD_FIELD",
    justification = "The closure classes trigger this. We never use serialization on this class")
public class ParquetMetadataStore {
  private final Map<S3URI, ColumnMappers> columnMappersStore;

  private final Map<Integer, List<String>> recentlyReadColumnsPerSchema;

  private final LogicalIOConfiguration configuration;

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
            new LinkedHashMap<Integer, List<String>>() {
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
   * @param configuration LogicalIO configuration
   * @param columnMappersStore Store of column mappings
   * @param recentlyReadColumnsPerSchema List of recent read columns for each schema
   */
  protected ParquetMetadataStore(
      LogicalIOConfiguration configuration,
      Map<S3URI, ColumnMappers> columnMappersStore,
      Map<Integer, List<String>> recentlyReadColumnsPerSchema) {
    this.configuration = configuration;
    this.columnMappersStore = columnMappersStore;
    this.recentlyReadColumnsPerSchema = recentlyReadColumnsPerSchema;
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
   * @param columnMetadata column to be added
   */
  public void addRecentColumn(ColumnMetadata columnMetadata) {

    List<String> schemaRecentColumns =
        recentlyReadColumnsPerSchema.getOrDefault(
            columnMetadata.getSchemaHash(), new ArrayList<>());

    if (schemaRecentColumns.size() == this.configuration.getMaxColumnAccessCountStoreSize()) {
      schemaRecentColumns.remove(0);
    }

    schemaRecentColumns.add(columnMetadata.getColumnName());

    recentlyReadColumnsPerSchema.put(columnMetadata.getSchemaHash(), schemaRecentColumns);
  }

  /**
   * Gets a list of unique column names from the recently read list.
   *
   * @param schemaHash the schema for which to retrieve columns for
   * @return Unique set of recently read columns
   */
  public Set<String> getUniqueRecentColumnsForSchema(int schemaHash) {
    List<String> schemaRecentColumns = recentlyReadColumnsPerSchema.get(schemaHash);

    if (schemaRecentColumns != null) {
      return new HashSet<>(schemaRecentColumns);
    }

    return Collections.emptySet();
  }
}
