package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.impl.ParquetMetadataStore;
import com.amazon.connector.s3.util.S3URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task for parsing the footer bytes to get the parquet FileMetaData and build maps which can be
 * used to track current columns being read. Best effort only, exceptions in parsing should be
 * suppressed by the calling class
 */
public class ParquetMetadataParsingTask {
  private final S3URI s3URI;
  private final ParquetParser parquetParser;
  private final ParquetMetadataStore parquetMetadataStore;
  private final LogicalIOConfiguration logicalIOConfiguration;
  private final ExecutorService asyncProcessingPool;

  private static final Logger LOG = LoggerFactory.getLogger(ParquetMetadataParsingTask.class);

  /**
   * Creates a new instance of {@link ParquetMetadataParsingTask}.
   *
   * @param s3URI the S3Uri of the object
   * @param logicalIOConfiguration logical io configuration
   * @param parquetMetadataStore object containing Parquet usage information
   * @param asyncProcessingPool
   */
  public ParquetMetadataParsingTask(
      S3URI s3URI,
      LogicalIOConfiguration logicalIOConfiguration,
      ParquetMetadataStore parquetMetadataStore,
      ExecutorService asyncProcessingPool) {
    this(
        s3URI,
        parquetMetadataStore,
        logicalIOConfiguration,
        new ParquetParser(logicalIOConfiguration),
        asyncProcessingPool);
  }

  /**
   * Creates a new instance of {@link ParquetMetadataParsingTask}. This version of the constructor
   * is useful for testing as it allows dependency injection.
   *
   * @param s3URI the S3Uri of the object
   * @param parquetMetadataStore object containing Parquet usage information
   * @param logicalIOConfiguration logical io configuration
   * @param parquetParser parser for getting the file metadata
   * @param asyncProcessingPool Custom thread pool for asynchronous processing
   */
  protected ParquetMetadataParsingTask(
      @NonNull S3URI s3URI,
      @NonNull ParquetMetadataStore parquetMetadataStore,
      @NonNull LogicalIOConfiguration logicalIOConfiguration,
      @NonNull ParquetParser parquetParser,
      @NonNull ExecutorService asyncProcessingPool) {
    this.s3URI = s3URI;
    this.parquetParser = parquetParser;
    this.parquetMetadataStore = parquetMetadataStore;
    this.logicalIOConfiguration = logicalIOConfiguration;
    this.asyncProcessingPool = asyncProcessingPool;
  }

  /**
   * Stores parquet metadata column mappings for future use
   *
   * @param fileTail tail of parquet file to be parsed
   * @return Column mappings
   */
  public ColumnMappers storeColumnMappers(FileTail fileTail) {

    try {
      CompletableFuture<FileMetaData> fileMetaData =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return parquetParser.parseParquetFooter(
                      fileTail.getFileTail(), fileTail.getFileTailLength());
                } catch (Exception e) {
                  e.printStackTrace();
                  throw new RuntimeException(e);
                }
              },
              asyncProcessingPool);
      ColumnMappers columnMappers =
          buildColumnMaps(
              fileMetaData.get(
                  LogicalIOConfiguration.DEFAULT.getParquetMetadataProcessingTimeoutMs(),
                  TimeUnit.MILLISECONDS));
      parquetMetadataStore.putColumnMappers(this.s3URI, columnMappers);
      return columnMappers;
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error(
          "Error parsing parquet footer for {}. Will fallback to synchronous reading for this key.",
          this.s3URI.getKey(),
          e);
      throw new CompletionException("Error parsing parquet footer", e);
    }
  }

  private ColumnMappers buildColumnMaps(FileMetaData fileMetaData) {

    HashMap<Long, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();
    HashMap<String, List<ColumnMetadata>> columnNameToColumnMap = new HashMap<>();

    int rowGroupIndex = 0;
    for (RowGroup rowGroup : fileMetaData.getRow_groups()) {

      for (ColumnChunk columnChunk : rowGroup.getColumns()) {

        // Get the full path to support nested schema
        String columnName = String.join(".", columnChunk.getMeta_data().getPath_in_schema());

        if (columnChunk.getMeta_data().getDictionary_page_offset() != 0) {
          ColumnMetadata columnMetadata =
              new ColumnMetadata(
                  rowGroupIndex,
                  columnName,
                  columnChunk.getMeta_data().getDictionary_page_offset(),
                  columnChunk.getMeta_data().getTotal_compressed_size());
          offsetIndexToColumnMap.put(
              columnChunk.getMeta_data().getDictionary_page_offset(), columnMetadata);
          List<ColumnMetadata> columnMetadataList =
              columnNameToColumnMap.computeIfAbsent(columnName, metadataList -> new ArrayList<>());
          columnMetadataList.add(columnMetadata);
        } else {
          ColumnMetadata columnMetadata =
              new ColumnMetadata(
                  rowGroupIndex,
                  columnName,
                  columnChunk.getFile_offset(),
                  columnChunk.getMeta_data().getTotal_compressed_size());
          offsetIndexToColumnMap.put(columnChunk.getFile_offset(), columnMetadata);
          List<ColumnMetadata> columnMetadataList =
              columnNameToColumnMap.computeIfAbsent(columnName, metadataList -> new ArrayList<>());
          columnMetadataList.add(columnMetadata);
        }
      }

      rowGroupIndex++;
    }

    return new ColumnMappers(offsetIndexToColumnMap, columnNameToColumnMap);
  }
}
