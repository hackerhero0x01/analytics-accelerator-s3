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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.utils.ImmutableMap;

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

  @ParameterizedTest
  @ValueSource(
      strings = {
        "src/test/resources/call_center_file_metadata.ser",
        "src/test/resources/nested_data_metadata.ser"
      })
  void testColumnMapCreation(String fileMetadata) throws IOException, ClassNotFoundException {

    PhysicalIO mockedPhysicalIO = mock(PhysicalIO.class);
    FileMetaData fileMetaData = getFileMetadata(fileMetadata);
    Optional<ColumnMappers> columnMappersOptional =
        getColumnMappers(fileMetaData, mockedPhysicalIO);

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
          String.join(".", columnChunk.getMeta_data().getPath_in_schema()),
          columnMappers.getOffsetIndexToColumnMap().get(key).getColumnName());
      assertEquals(
          columnChunk.getMeta_data().getTotal_compressed_size(),
          columnMappers.getOffsetIndexToColumnMap().get(key).getCompressedSize());
    }
  }

  @ParameterizedTest
  @MethodSource("arguments")
  void testColumnMapCreationMultiRowGroup(
      String filename, Map<String, Integer> expectedColToRowGroup, int expectedColumns)
      throws IOException, ClassNotFoundException {
    // Deserialize fileMetaData object
    PhysicalIO mockedPhysicalIO = mock(PhysicalIO.class);
    FileMetaData fileMetaData = getFileMetadata(filename);
    Optional<ColumnMappers> columnMappersOptional =
        getColumnMappers(fileMetaData, mockedPhysicalIO);

    assertTrue(columnMappersOptional.isPresent());

    ColumnMappers columnMappers = columnMappersOptional.get();
    HashMap<String, List<ColumnMetadata>> columnNameToColumnMap =
        columnMappers.getColumnNameToColumnMap();

    // parquet file "multi_row_group.parquet" in resources has 2 columns and 3 row groups. So the
    // map
    // should have two entries (one for each column) of size 3 (one entry for each occurrence of the
    // column)
    assertEquals(expectedColumns, columnNameToColumnMap.size());
    expectedColToRowGroup.forEach(
        (col, rowGroup) -> {
          assertEquals(rowGroup, columnNameToColumnMap.get(col).size());
        });

    int rowGroupIndex = 0;
    for (RowGroup rowGroup : fileMetaData.getRow_groups()) {
      assertEquals(rowGroup.getColumns().size(), columnNameToColumnMap.size());
      for (ColumnChunk columnChunk : rowGroup.getColumns()) {
        List<ColumnMetadata> columnMetadataList =
            columnNameToColumnMap.get(
                String.join(".", columnChunk.getMeta_data().getPath_in_schema()));
        ColumnMetadata columnMetadata = columnMetadataList.get(rowGroupIndex);

        assertEquals(
            columnMetadata.getColumnName(),
            String.join(".", columnChunk.getMeta_data().getPath_in_schema()));

        // If the column has a dictionary, start pos should be equal to dictionary_page_offset as
        // this is
        // where reads for this column start.
        long startPos;
        if (columnChunk.getMeta_data().getDictionary_page_offset() != 0) {
          startPos = columnChunk.getMeta_data().getDictionary_page_offset();
        } else {
          startPos = columnChunk.getFile_offset();
        }

        assertEquals(startPos, columnMetadata.getStartPos());
        assertEquals(rowGroupIndex, columnMetadata.getRowGroupIndex());
      }
      rowGroupIndex++;
    }
  }

  private static Stream<Arguments> arguments() {
    return Stream.of(
        Arguments.of(
            "src/test/resources/multi_row_group.ser", ImmutableMap.of("n_legs", 3, "animal", 3), 2),
        Arguments.of(
            "src/test/resources/nested_data_mrg_metadata.ser",
            ImmutableMap.of(
                "address.city",
                3,
                "address.zip",
                3,
                "phone_numbers.list.element.type",
                3,
                "phone_numbers.list.element.number",
                3),
            8));
  }

  @Test
  void testColumnMapCreationNestedSchema() throws IOException, ClassNotFoundException {
    // Deserialize fileMetaData object
    PhysicalIO mockedPhysicalIO = mock(PhysicalIO.class);
    FileMetaData fileMetaData = getFileMetadata("src/test/resources/nested_data_metadata.ser");
    Optional<ColumnMappers> columnMappersOptional =
        getColumnMappers(fileMetaData, mockedPhysicalIO);

    assertTrue(columnMappersOptional.isPresent());

    ColumnMappers columnMappers = columnMappersOptional.get();
    HashMap<String, List<ColumnMetadata>> columnNameToColumnMap =
        columnMappers.getColumnNameToColumnMap();
    assertEquals(8, columnNameToColumnMap.size());

    // The underlying data is nested for address and phone number.
    // Eg:   {
    //        'name': 'John Doe',
    //        'age': 30,
    //        'address': {'street': '123 Main St', 'city': 'Anytown', 'state': 'CA', 'zip': 12345},
    //        'phone_numbers': [{'type': 'home', 'number': '555-1234'}, {'type': 'work', 'number':
    // '555-5678'}]
    //    },
    //    {
    //        'name': 'Jane Smith',
    //        'age': 25,
    //        'address': {'street': '456 Maple Ave', 'city': 'Othertown', 'state': 'NY', 'zip':
    // 67890},
    //        'phone_numbers': [{'type': 'home', 'number': '555-8765'}]
    //    }

    // Check the path of the nested columns
    assertTrue(columnNameToColumnMap.containsKey("address.street"));
    assertTrue(columnNameToColumnMap.containsKey("address.zip"));
    assertTrue(columnNameToColumnMap.containsKey("phone_numbers.list.element.type"));
    assertTrue(columnNameToColumnMap.containsKey("phone_numbers.list.element.number"));
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

  private FileMetaData getFileMetadata(String filePath) throws IOException, ClassNotFoundException {
    // Deserialize fileMetaData object
    FileInputStream fileInStream = new FileInputStream(filePath);
    ObjectInputStream ois = new ObjectInputStream(fileInStream);
    FileMetaData fileMetaData = (FileMetaData) ois.readObject();
    ois.close();

    return fileMetaData;
  }

  private Optional<ColumnMappers> getColumnMappers(
      FileMetaData fileMetaData, PhysicalIO mockedPhysicalIO) throws IOException {

    ParquetParser mockedParquetParser = mock(ParquetParser.class);
    when(mockedParquetParser.parseParquetFooter(any(ByteBuffer.class), anyInt()))
        .thenReturn(fileMetaData);

    ParquetMetadataTask parquetMetadataTask =
        new ParquetMetadataTask(
            mockedPhysicalIO, LogicalIOConfiguration.DEFAULT, mockedParquetParser);

    return parquetMetadataTask.storeColumnMappers(
        Optional.of(new FileTail(ByteBuffer.allocate(0), 0)));
  }
}
