package com.amazon.connector.s3.io.logical.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.junit.jupiter.api.Test;

public class ParquetMetadataTaskTest {

  @Test
  void testContructor() {
    assertNotNull(new ParquetMetadataTask(LogicalIOConfiguration.DEFAULT, mock(PhysicalIO.class)));
  }

  @Test
  void testConstructorFailsOnNull() {
    assertThrows(
        NullPointerException.class,
        () -> new ParquetMetadataTask(LogicalIOConfiguration.DEFAULT, null));
    assertThrows(
        NullPointerException.class, () -> new ParquetMetadataTask(null, mock(PhysicalIO.class)));
  }

  @Test
  void testColumnMapCreation() throws IOException, ClassNotFoundException {
    // Deserialize fileMetaData object
    FileInputStream fileInStream =
        new FileInputStream("src/test/resources/call_center_file_metadata.ser");
    ObjectInputStream ois = new ObjectInputStream(fileInStream);
    FileMetaData fileMetaData = (FileMetaData) ois.readObject();
    ois.close();

    ParquetParser mockedParquetParser = mock(ParquetParser.class);
    when(mockedParquetParser.parseParquetFooter(any(ByteBuffer.class), anyInt()))
        .thenReturn(fileMetaData);

    PhysicalIO mockedPhysicalIO = mock(PhysicalIO.class);

    ParquetMetadataTask parquetMetadataTask =
        new ParquetMetadataTask(
            mockedPhysicalIO, LogicalIOConfiguration.DEFAULT, mockedParquetParser);

    Optional<ColumnMappers> columnMappersOptional =
        CompletableFuture.supplyAsync(
                () ->
                    parquetMetadataTask.storeColumnMappers(
                        Optional.of(new FileTail(ByteBuffer.allocate(0), 0))))
            .join();

    assertTrue(columnMappersOptional.isPresent());

    ColumnMappers columnMappers = columnMappersOptional.get();
    assertEquals(
        fileMetaData.getRow_groups().get(0).getColumns().size(),
        columnMappers.getOffsetIndexToColumnMap().size());

    verify(mockedPhysicalIO).putColumnMappers(any(ColumnMappers.class));

    for (ColumnChunk columnChunk : fileMetaData.getRow_groups().get(0).getColumns()) {
      Long key;

      // If the column has a dictionary, key should be equal to dictionary_page_offset as this is
      // where reads for this column start.
      if (columnChunk.getMeta_data().getDictionary_page_offset() != 0) {
        key = columnChunk.getMeta_data().getDictionary_page_offset();
      } else {
        key = columnChunk.getFile_offset();
      }

      assertTrue(columnMappers.getOffsetIndexToColumnMap().containsKey(key));
      assertEquals(0, columnMappers.getOffsetIndexToColumnMap().get(key).getRowGroupIndex());
      assertEquals(
          columnChunk.getMeta_data().getPath_in_schema().get(0),
          columnMappers.getOffsetIndexToColumnMap().get(key).getColumnName());
      assertEquals(
          columnChunk.getMeta_data().getTotal_compressed_size(),
          columnMappers.getOffsetIndexToColumnMap().get(key).getCompressedSize());
    }
  }

  @Test
  void testParsingExceptionsSwallowed() throws IOException {
    ParquetParser mockedParquetParser = mock(ParquetParser.class);
    when(mockedParquetParser.parseParquetFooter(any(ByteBuffer.class), anyInt()))
        .thenThrow(new IOException("can not read FileMetaData"));
    ParquetMetadataTask parquetMetadataTask =
        new ParquetMetadataTask(
            mock(PhysicalIO.class), LogicalIOConfiguration.DEFAULT, mockedParquetParser);
    CompletableFuture<Optional<ColumnMappers>> parquetMetadataTaskFuture =
        CompletableFuture.supplyAsync(
            () ->
                parquetMetadataTask.storeColumnMappers(
                    Optional.of(new FileTail(ByteBuffer.allocate(0), 0))));

    // Any errors in parsing should be swallowed
    assertFalse(parquetMetadataTaskFuture.join().isPresent());
  }

  @Test
  void testEmptyFileTail() {
    ParquetMetadataTask parquetMetadataTask =
        new ParquetMetadataTask(
            mock(PhysicalIO.class), LogicalIOConfiguration.DEFAULT, mock(ParquetParser.class));

    assertFalse(parquetMetadataTask.storeColumnMappers(Optional.empty()).isPresent());
  }
}
