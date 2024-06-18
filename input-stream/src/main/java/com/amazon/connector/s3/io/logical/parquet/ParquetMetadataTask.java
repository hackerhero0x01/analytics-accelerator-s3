package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import java.io.IOException;
import java.util.HashMap;
import lombok.NonNull;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task for parsing the footer bytes to get the parquet FileMetaData and build maps which can be
 * used to track current columns being read. Best effort only, any exceptions in parsing will be
 * swallowed.
 */
public class ParquetMetadataTask {
  private final ParquetParser parquetParser;
  private final PhysicalIO physicalIO;
  private final LogicalIOConfiguration logicalIOConfiguration;

  private static final Logger LOG = LoggerFactory.getLogger(ParquetMetadataTask.class);

  /**
   * Creates a new instance of {@link ParquetMetadataTask}.
   *
   * @param physicalIO PhysicalIO instance
   * @param logicalIOConfiguration logical io configuration
   */
  public ParquetMetadataTask(LogicalIOConfiguration logicalIOConfiguration, PhysicalIO physicalIO) {
    this(physicalIO, logicalIOConfiguration, new ParquetParser());
  }

  /**
   * Creates a new instance of {@link ParquetMetadataTask}. This version of the constructor is
   * useful for testing as it allows dependency injection.
   *
   * @param physicalIO PhysicalIO instance
   * @param logicalIOConfiguration logical io configuration
   * @param parquetParser parser for getting the file metadata
   */
  protected ParquetMetadataTask(
      @NonNull PhysicalIO physicalIO,
      @NonNull LogicalIOConfiguration logicalIOConfiguration,
      @NonNull ParquetParser parquetParser) {
    this.parquetParser = parquetParser;
    this.physicalIO = physicalIO;
    this.logicalIOConfiguration = logicalIOConfiguration;
  }

  /**
   * Stores parquet metadata column mappings for future use
   *
   * @param fileTail tail of parquet file to be parsed
   * @return Column mappings
   */
  public ColumnMappers storeColumnMappers(FileTail fileTail) {
    try {
      FileMetaData fileMetaData =
          parquetParser.parseParquetFooter(fileTail.getFileTail(), fileTail.getFileTailLength());
      ColumnMappers columnMappers = buildColumnMaps(fileMetaData);
      physicalIO.putColumnMappers(columnMappers);
      return columnMappers;
    } catch (IOException e) {
      LOG.debug("Error parsing parquet footer", e);
      return null;
    }
  }

  private ColumnMappers buildColumnMaps(FileMetaData fileMetaData) {

    HashMap<String, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();

    int rowGroupIndex = 0;
    for (RowGroup rowGroup : fileMetaData.getRow_groups()) {

      for (ColumnChunk columnChunk : rowGroup.getColumns()) {
        // TODO: This bit in particular seems very brittle, need to figure out "path in schema"
        // means.
        String columnName = columnChunk.getMeta_data().getPath_in_schema().get(0);

        if (columnChunk.getMeta_data().getDictionary_page_offset() != 0) {
          offsetIndexToColumnMap.put(
              Long.toString(columnChunk.getMeta_data().getDictionary_page_offset()),
              new ColumnMetadata(
                  rowGroupIndex,
                  columnName,
                  columnChunk.getMeta_data().getDictionary_page_offset(),
                  columnChunk.getMeta_data().getTotal_compressed_size()));
        } else {
          offsetIndexToColumnMap.put(
              Long.toString(columnChunk.getFile_offset()),
              new ColumnMetadata(
                  rowGroupIndex,
                  columnName,
                  columnChunk.getFile_offset(),
                  columnChunk.getMeta_data().getTotal_compressed_size()));
        }
      }

      rowGroupIndex++;
    }

    return new ColumnMappers(offsetIndexToColumnMap);
  }
}
