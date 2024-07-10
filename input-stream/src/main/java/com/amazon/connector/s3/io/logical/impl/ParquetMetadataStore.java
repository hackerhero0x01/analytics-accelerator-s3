package com.amazon.connector.s3.io.logical.impl;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.io.logical.parquet.ParquetPrefetchTailTask;
import com.amazon.connector.s3.util.S3URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazon.connector.s3.util.Constants.DEFAULT_PARQUET_METADATA_STORE_SIZE;

/** Object to aggregate column usage statistics from Parquet files */
public class ParquetMetadataStore {

  private final LogicalIOConfiguration configuration;

  private final Map<S3URI, ColumnMappers> columnMappersStore;

  private final Map<String, Integer> recentColumns;

  private AtomicInteger cleanCounter = new AtomicInteger(0);

  private static final Logger LOG = LoggerFactory.getLogger(ParquetMetadataStore.class);

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
    synchronized (recentColumns) {
      int columnAccessCount = recentColumns.getOrDefault(columnName, 0);
      LOG.info("ADDDING RECENT COLUMN {}", columnName);
      recentColumns.put(columnName, columnAccessCount + 1);
      maybeCleanRecentColumnList();
    }

  }

  private void maybeCleanRecentColumnList() {
    int currentCleanCount = cleanCounter.incrementAndGet();

    LOG.info("Incrementing counter, current count {}", currentCleanCount);

    synchronized (recentColumns) {
      if (currentCleanCount == 20) {
        LOG.info("Cleaning recent columns list");
        Iterator<Map.Entry<String,Integer>> iter = recentColumns.entrySet().iterator();
        while (iter.hasNext()) {
          Map.Entry<String,Integer> entry = iter.next();
          LOG.info("RECENT COLUMNS IN LIST {}", entry.getKey());
          if (entry.getValue() < 3) {
            LOG.info("REMOVING COLUMN {} with CoUNT {}", entry.getKey(), entry.getValue());
            iter.remove();
          }
        }
        cleanCounter.set(0);
      }
    }
  }

  /**
   * Gets a list of recent columns being read.
   *
   * @return Set of recent columns being
   */
  public Set<String> getRecentColumns() {
    return recentColumns.keySet();
  }
}
