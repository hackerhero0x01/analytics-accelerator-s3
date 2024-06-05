package com.amazon.connector.s3.io.logical.parquet;

import static com.amazon.connector.s3.util.Constants.PARQUET_FOOTER_LENGTH_SIZE;
import static com.amazon.connector.s3.util.Constants.PARQUET_MAGIC_STR_LENGTH;

import com.amazon.connector.s3.common.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.function.Supplier;
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
public class ParquetMetadataTask implements Supplier<Void> {
  private final byte[] fileTail;
  private final int tailLength;
  private final HashMap<String, ColumnMetadata> offsetIndexToColumnMap;
  private final ParquetParser parquetParser;

  private static final Logger LOG = LoggerFactory.getLogger(ParquetMetadataTask.class);

  /**
   * Creates a new instance of {@link ParquetMetadataTask}.
   *
   * @param fileTail buffer with footer bytes to be parsed
   * @param tailLength the length of data to be parsed
   * @param columnMappers internal mappers to hold column to file offset index mappings
   */
  public ParquetMetadataTask(
      @NonNull byte[] fileTail, int tailLength, @NonNull ColumnMappers columnMappers) {

    Preconditions.checkArgument(
        tailLength > PARQUET_MAGIC_STR_LENGTH + PARQUET_FOOTER_LENGTH_SIZE,
        "Specified content length is too low");

    this.fileTail = fileTail;
    this.tailLength = tailLength;
    this.offsetIndexToColumnMap = columnMappers.getOffsetIndexToColumnMap();
    this.parquetParser = new ParquetParser(ByteBuffer.wrap(fileTail));
  }

  /**
   * Creates a new instance of {@link ParquetMetadataTask}. This version of the constructor is
   * useful for testing as it allows dependency injection.
   *
   * @param fileTail buffer with footer bytes to be parsed
   * @param tailLength the length of data to be parsed
   * @param columnMappers internal mappers to hold column to file offset index mappings
   * @param parquetParser parser for getting the file metadata
   */
  protected ParquetMetadataTask(
      @NonNull byte[] fileTail,
      int tailLength,
      @NonNull ColumnMappers columnMappers,
      @NonNull ParquetParser parquetParser) {
    this.fileTail = fileTail;
    this.tailLength = tailLength;
    this.offsetIndexToColumnMap = columnMappers.getOffsetIndexToColumnMap();
    this.parquetParser = parquetParser;
  }

  @Override
  public Void get() {
    try {
      FileMetaData fileMetaData = parquetParser.parseParquetFooter(tailLength);
      buildColumnMaps(fileMetaData);
    } catch (IOException e) {
      LOG.debug("Error parsing parquet footer", e);
    }
    return null;
  }

  private void buildColumnMaps(FileMetaData fileMetaData) {

    int rowGroupIndex = 0;
    for (RowGroup rowGroup : fileMetaData.getRow_groups()) {

      for (ColumnChunk columnChunk : rowGroup.getColumns()) {
        // TODO: This bit in particular seems very brittle, need to figure out "path in schema"
        // means.
        String columnName = columnChunk.getMeta_data().getPath_in_schema().get(0);

        if (columnChunk.getMeta_data().getDictionary_page_offset() != 0) {
          this.offsetIndexToColumnMap.put(
              Long.toString(columnChunk.getMeta_data().getDictionary_page_offset()),
              new ColumnMetadata(
                  rowGroupIndex,
                  columnName,
                  columnChunk.getMeta_data().getDictionary_page_offset(),
                  columnChunk.getMeta_data().getTotal_compressed_size()));
        } else {
          this.offsetIndexToColumnMap.put(
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
  }
}
