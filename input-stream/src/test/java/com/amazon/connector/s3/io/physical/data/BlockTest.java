package com.amazon.connector.s3.io.physical.data;

import static org.junit.jupiter.api.Assertions.*;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.TestTelemetry;
import com.amazon.connector.s3.request.ReadMode;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import org.junit.jupiter.api.Test;

public class BlockTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  public void testSingleByteReadReturnsCorrectByte() {
    // Given: a Block containing "test-data"
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    Block block =
        new Block(
            TEST_URI,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC);

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
  public void testBufferedReadReturnsCorrectBytes() {
    // Given: a Block containing "test-data"
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    Block block =
        new Block(
            TEST_URI,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC);

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

  @Test
  void testNulls() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    assertThrows(
        NullPointerException.class,
        () ->
            new Block(
                null,
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                0,
                TEST_DATA.length(),
                0,
                ReadMode.SYNC));
    assertThrows(
        NullPointerException.class,
        () ->
            new Block(
                TEST_URI, null, TestTelemetry.DEFAULT, 0, TEST_DATA.length(), 0, ReadMode.SYNC));
    assertThrows(
        NullPointerException.class,
        () -> new Block(TEST_URI, fakeObjectClient, null, 0, TEST_DATA.length(), 0, ReadMode.SYNC));
    assertThrows(
        NullPointerException.class,
        () ->
            new Block(
                TEST_URI, fakeObjectClient, TestTelemetry.DEFAULT, 0, TEST_DATA.length(), 0, null));
  }

  @Test
  void testBoundaries() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(
                TEST_URI,
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                -1,
                TEST_DATA.length(),
                0,
                ReadMode.SYNC));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(TEST_URI, fakeObjectClient, TestTelemetry.DEFAULT, 0, -5, 0, ReadMode.SYNC));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(TEST_URI, fakeObjectClient, TestTelemetry.DEFAULT, 20, 1, 0, ReadMode.SYNC));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(TEST_URI, fakeObjectClient, TestTelemetry.DEFAULT, 0, 5, -1, ReadMode.SYNC));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new Block(
                TEST_URI,
                fakeObjectClient,
                TestTelemetry.DEFAULT,
                -5,
                0,
                TEST_DATA.length(),
                ReadMode.SYNC));
  }

  @Test
  void testReadBoundaries() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    byte[] b = new byte[4];
    Block block =
        new Block(
            TEST_URI,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC);
    assertThrows(IllegalArgumentException.class, () -> block.read(-10));
    assertThrows(NullPointerException.class, () -> block.read(null, 0, 3, 1));
    assertThrows(IllegalArgumentException.class, () -> block.read(b, -5, 3, 1));
    assertThrows(IllegalArgumentException.class, () -> block.read(b, 0, -5, 1));
    assertThrows(IllegalArgumentException.class, () -> block.read(b, 10, 3, 1));
  }

  @Test
  void testContains() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    Block block =
        new Block(
            TEST_URI,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC);
    assertTrue(block.contains(0));
    assertFalse(block.contains(TEST_DATA.length() + 1));
  }

  @Test
  void testContainsBoundaries() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    Block block =
        new Block(
            TEST_URI,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC);
    assertThrows(IllegalArgumentException.class, () -> block.contains(-1));
  }

  @Test
  void testClose() {
    final String TEST_DATA = "test-data";
    ObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
    Block block =
        new Block(
            TEST_URI,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            0,
            TEST_DATA.length(),
            0,
            ReadMode.SYNC);
    block.close();
    block.close();
  }
}
