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

/**
 * This class maintains a shared state required for Parquet prefetching operations that is required
 * independent of the life of individual streams. It is used to store Parquet metadata for
 * individual files, and a list of recently read columns. This must be done outside the life of a
 * stream, as calling applications may open and close a stream to a file several times while
 * reading. For Spark, this was observed to happen as a stream to a Parquet file is first opened to
 * read the footer, and then a separate stream is opened to read the data.
 */
@SuppressFBWarnings(
    value = "SE_BAD_FIELD",
    justification = "The closure classes trigger this. We never use serialization on this class")
public class ParquetMetadataStore {

  private final Map<S3URI, ColumnMappers> columnMappersStore;

  /**
   * This is a mapping of schema and the recently read columns for it. For a Parquet file, a hash is
   * calculated by concatenating all the column names in the file metadata into a single string, and
   * then computing the hash. This helps separate all Parquet files belonging to the same table. Eg:
   * Two files belonging to store_sales table will have the same columns, and so have the same
   * schema hash.
   *
   * <p>This map is then used to store a list of recently read columns for each schema. For example,
   * if a stream recently read columns [ss_a, ss_b] for a store_sales schema, the list would contain
   * [ss_a, ss_b]. The list is limited to a size defined by maxColumnAccessCountStoreSize in {@link
   * LogicalIOConfiguration}. By default, this 15. This means that this list will contain the last
   * 15 columns that were read for this particular schema.
   *
   * <p>If a query is reading ss_a and ss_b, this list will look something like [ss_a, ss_b, ss_a,
   * ss_b]. This helps us maintain a history of the columns currently being read.
   */
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
        new LinkedHashMap<S3URI, ColumnMappers>() {
          @Override
          protected boolean removeEldestEntry(final Map.Entry eldest) {
            return this.size() > configuration.getParquetMetadataStoreSize();
          }
        },
        new LinkedHashMap<Integer, List<String>>() {
          @Override
          protected boolean removeEldestEntry(final Map.Entry eldest) {
            return this.size() > configuration.getMaxColumnAccessCountStoreSize();
          }
        });
  }

  /**
   * Creates a new instance of ParquetMetadataStore. This constructor is used for dependency
   * injection.
   *
   * @param configuration LogicalIO configuration
   * @param columnMappersStore Store of column mappings
   * @param recentlyReadColumnsPerSchema List of recent read columns for each schema
   */
  ParquetMetadataStore(
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
  public synchronized ColumnMappers getColumnMappers(S3URI s3URI) {
    return columnMappersStore.get(s3URI);
  }

  /**
   * Stores column mappers for an object.
   *
   * @param s3URI S3URI to store mappers for
   * @param columnMappers Parquet metadata column mappings
   */
  public synchronized void putColumnMappers(S3URI s3URI, ColumnMappers columnMappers) {
    columnMappersStore.put(s3URI, columnMappers);
  }

  /**
   * Adds a column to the list of recent columns for a particular schema. This is a fixed sized
   * list, whose size is defined by maxColumnAccessCountStoreSize in {@link LogicalIOConfiguration}.
   * The default value is 15.
   *
   * <p>Reads at particular file offset correspond to a specific column being read. When a read
   * happens, {@link ColumnMappers} are used to find if this read corresponds to a column for the
   * currently open Parquet file. When a read happens, {@code
   * ParquetPredictivePrefetchingTask.addToRecentColumnList()} is used to decipher if it corresponds
   * to a column, that is, is there a column in the Parquet file with the same file_offset as the
   * current position of the stream? If yes, this column gets added to the recently read columns for
   * that particular schema. All Parquet files that have the exact same columns, and so the same
   * hash(concatenated string of columnNames), are said to belong to the same schema eg:
   * "store_sales".
   *
   * <p>This list maintains names of the last 15 columns that were read for a schema. This is used
   * to maintain a brief recent history of the columns being read by a workload and make predictions
   * when prefetching. Only columns that exist in this list are prefetched by {@link
   * com.amazon.connector.s3.io.logical.parquet.ParquetPredictivePrefetchingTask}.
   *
   * <p>For example, assume that this list is of size 3, and the current query executing is Select
   * ss_a, ss_b from store_sales. This list will then contain something like [ss_a, ss_b, ss_a]. If
   * the query changes to Select ss_d, ss_e from store_sales, this list will start getting updated
   * as the reads for column ss_d and ss_e are requested.
   *
   * <p>The list will change as follows: // new read for ss_d is requested, update list. Since list
   * is already at capacity, remove the first element from it as this the column we saw least
   * recently. The list then becomes: [ss_b, ss_a, ss_d]
   *
   * <p>// A read for ss_e comes in, update list. Again, since list is at capacity, remove the least
   * recently seen column. List becomes: [ss_a, ss_d, ss_e]
   *
   * <p>After the next read, list will be [ss_d, ss_e, ss_d]
   *
   * <p>In this way, a fixed size list helps maintain the most recent columns and increases accuracy
   * when prefetching.
   *
   * @param columnMetadata column to be added
   */
  public synchronized void addRecentColumn(ColumnMetadata columnMetadata) {

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
   * Gets a list of unique column names from the recently read list. The recently read list contains
   * a history of recent columns read for a particular schema. For example, for a store_sales schema
   * this can be [ss_a, ss_b, ss_a, ss_b, ss_c, ss_c]. For prefetching, a set of unique columns to
   * prefetch from this list is required. In this case, this will [ss_a, ss_b, ss_c].
   *
   * @param schemaHash the schema for which to retrieve columns for
   * @return Unique set of recently read columns
   */
  public synchronized Set<String> getUniqueRecentColumnsForSchema(int schemaHash) {
    List<String> schemaRecentColumns = recentlyReadColumnsPerSchema.get(schemaHash);

    if (schemaRecentColumns != null) {
      return new HashSet<>(schemaRecentColumns);
    }

    return Collections.emptySet();
  }
}
