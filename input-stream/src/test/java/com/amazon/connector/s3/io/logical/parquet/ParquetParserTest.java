package com.amazon.connector.s3.io.logical.parquet;

import static com.amazon.connector.s3.util.Constants.ONE_KB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.apache.parquet.format.FileMetaData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ParquetParserTest {

  @Test
  void testContructor() {
    assertNotNull(new ParquetParser());
  }

  @ParameterizedTest
  @MethodSource("arguments")
  void testParquetMetadataParsing(String parquetFilePath, int expectedColumns) throws IOException {

    File file = new File(parquetFilePath);
    InputStream inputStream = new FileInputStream(file);

    byte[] buffer = new byte[ONE_KB * 20];
    inputStream.read(buffer, 0, (int) file.length());

    ParquetParser parquetParser = new ParquetParser();
    FileMetaData fileMetaData =
        parquetParser.parseParquetFooter(ByteBuffer.wrap(buffer), (int) file.length());

    assertEquals(fileMetaData.row_groups.size(), 1);
    assertEquals(fileMetaData.getRow_groups().get(0).getColumns().size(), expectedColumns);
  }

  private static Stream<Arguments> arguments() {
    return Stream.of(
        Arguments.of("src/test/resources/call_center.parquet", 31),
        Arguments.of("src/test/resources/nested_data.parquet", 8));
  }
  @Test
  void testParquetMetadataParsingMultipleRowGroups() throws IOException {

    File file = new File("src/test/resources/multi_row_group.parquet");
    InputStream inputStream = new FileInputStream(file);

    byte[] buffer = new byte[ONE_KB * 20];
    inputStream.read(buffer, 0, (int) file.length());

    ParquetParser parquetParser = new ParquetParser();
    FileMetaData fileMetaData =
        parquetParser.parseParquetFooter(ByteBuffer.wrap(buffer), (int) file.length());

    assertEquals(fileMetaData.row_groups.size(), 3);
    assertEquals(fileMetaData.getRow_groups().get(0).getColumns().size(), 2);
  }

  @Test
  void testParquetMetadataParsingInvalidData() {

    ParquetParser parquetParserInvalidLength = new ParquetParser();
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          parquetParserInvalidLength.parseParquetFooter(ByteBuffer.allocate(ONE_KB), 8);
        });

    // Empty buffer, will throw thrift exception
    ParquetParser parquetParserInvalidBuffer = new ParquetParser();
    assertThrows(
        IOException.class,
        () -> {
          parquetParserInvalidBuffer.parseParquetFooter(ByteBuffer.allocate(ONE_KB), 9);
        });
  }
}
