package com.amazon.connector.s3.io.logical.parquet;

import static com.amazon.connector.s3.util.Constants.ONE_KB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.concurrent.CompletableFuture;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.junit.jupiter.api.Test;

public class ParquetMetadataTaskTest {

  @Test
  void testContructor() {
    assertNotNull(new ParquetMetadataTask(new byte[ONE_KB], ONE_KB, new ColumnMappers()));
  }

  @Test
  void testConstructorFailsOnNull() {
    assertThrows(
        NullPointerException.class,
        () -> new ParquetMetadataTask(null, ONE_KB, new ColumnMappers()));
    assertThrows(
        NullPointerException.class, () -> new ParquetMetadataTask(new byte[ONE_KB], ONE_KB, null));
  }

  @Test
  void testInvalidTailLength() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ParquetMetadataTask(new byte[ONE_KB], 8, new ColumnMappers()));
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
    when(mockedParquetParser.parseParquetFooter(anyInt())).thenReturn(fileMetaData);

    ColumnMappers columnMappers = new ColumnMappers();
    ParquetMetadataTask parquetMetadataTask =
        new ParquetMetadataTask(new byte[ONE_KB], ONE_KB, columnMappers, mockedParquetParser);
    CompletableFuture<Void> parquetMetadataTaskFuture =
        CompletableFuture.supplyAsync(parquetMetadataTask);
    parquetMetadataTaskFuture.join();

    assertEquals(
        fileMetaData.getRow_groups().get(0).getColumns().size(),
        columnMappers.getOffsetIndexToColumnMap().size());

    for (ColumnChunk columnChunk : fileMetaData.getRow_groups().get(0).getColumns()) {
      String key;

      if (columnChunk.getMeta_data().getDictionary_page_offset() != 0) {
        key = Long.toString(columnChunk.getMeta_data().getDictionary_page_offset());
      } else {
        key = Long.toString(columnChunk.getFile_offset());
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
    when(mockedParquetParser.parseParquetFooter(anyInt()))
        .thenThrow(new IOException("can not read FileMetaData"));
    ParquetMetadataTask parquetMetadataTask =
        new ParquetMetadataTask(new byte[ONE_KB], ONE_KB, new ColumnMappers(), mockedParquetParser);
    CompletableFuture<Void> parquetMetadataTaskFuture =
        CompletableFuture.supplyAsync(parquetMetadataTask);

    // Any errors in parsing should be swallowed
    assertNull(parquetMetadataTaskFuture.join());
  }
}
