package com.amazon.connector.s3.io.physical.data;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.request.ReadMode;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import org.junit.jupiter.api.Test;

public class BlockTest {

  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  public void test__singleByteRead__returnsCorrectByte() {
    // Given: a Block containing "test-data"
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    Block block = new Block(TEST_URI, fakeObjectClient, Telemetry.NOOP, 0, TEST_DATA.length(), 0, ReadMode.SYNC);

    // When: bytes are requested from the block
    int r1 = block.read(0);
    int r2 = block.read(TEST_DATA.length() - 1);
    int r3 = block.read(4);

    // Then: they are the correct bytes
    assertEquals(116, r1); // 't' = 116
    assertEquals(97, r2); // 'a' = 97
    assertEquals(45, r3); // '-' = 45
  }

  @Test
  public void test__bufferedRead__returnsCorrectBytes() {
    // Given: a Block containing "test-data"
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    Block block = new Block(TEST_URI, fakeObjectClient, Telemetry.NOOP, 0, TEST_DATA.length(), 0, ReadMode.SYNC);

    // When: bytes are requested from the block
    byte[] b1 = new byte[4];
    int r1 = block.read(b1, 0, b1.length, 0);
    byte[] b2 = new byte[4];
    int r2 = block.read(b2, 0, b2.length, 5);

    // Then: they are the correct bytes
    assertEquals(4, r1);
    assertEquals("test", new String(b1));

    assertEquals(4, r2);
    assertEquals("data", new String(b2));
  }
}
