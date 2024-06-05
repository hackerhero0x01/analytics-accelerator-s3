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
import org.apache.parquet.format.FileMetaData;
import org.junit.jupiter.api.Test;

public class ParquetParserTest {

  @Test
  void testContructor() {
    assertNotNull(new ParquetParser(ByteBuffer.allocate(0)));
  }

  @Test
  void testConstructorFailsOnNull() {
    assertThrows(NullPointerException.class, () -> new ParquetParser(null));
  }

  @Test
  void testParquetMetadataParsing() throws IOException {

    File file = new File("src/test/resources/call_center.parquet");
    InputStream inputStream = new FileInputStream(file);

    byte[] buffer = new byte[ONE_KB * 20];
    inputStream.read(buffer, 0, (int) file.length());

    ParquetParser parquetParser = new ParquetParser(ByteBuffer.wrap(buffer));
    FileMetaData fileMetaData = parquetParser.parseParquetFooter((int) file.length());

    assertEquals(fileMetaData.row_groups.size(), 1);
    assertEquals(fileMetaData.getRow_groups().get(0).getColumns().size(), 31);
  }

  @Test
  void testParquetMetadataParsingInvalidData() {

    ParquetParser parquetParserInvalidLength = new ParquetParser(ByteBuffer.allocate(ONE_KB));
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          parquetParserInvalidLength.parseParquetFooter(8);
        });

    // Empty buffer, will throw thrift exception
    ParquetParser parquetParserInvalidBuffer = new ParquetParser(ByteBuffer.allocate(ONE_KB));
    assertThrows(
        IOException.class,
        () -> {
          parquetParserInvalidBuffer.parseParquetFooter(9);
        });
  }
}
