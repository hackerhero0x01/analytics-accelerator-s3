package com.amazon.connector.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.amazon.connector.s3.blockmanager.BlockManager;
import com.amazon.connector.s3.util.S3URI;
import java.io.EOFException;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.utils.IoUtils;

public class S3SeekableInputStreamTest extends S3SeekableInputStreamTestBase {

  @Test
  void testConstructor() throws IOException {
    S3SeekableInputStream inputStream = new S3SeekableInputStream(fakeBlockManager);
    assertNotNull(inputStream);
  }

  @Test
  void testDefaultConstructor() throws IOException {
    S3SeekableInputStream inputStream = new S3SeekableInputStream(S3URI.of("bucket", "key"));
    assertNotNull(inputStream);
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new S3SeekableInputStream((S3URI) null);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new S3SeekableInputStream((BlockManager) null);
        });
  }

  @Test
  void testInitialGetPosition() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(fakeBlockManager);

    // When: nothing
    // Then: stream position is at 0
    assertEquals(0, stream.getPos());
  }

  @Test
  void testReadAdvancesPosition() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(fakeBlockManager);

    // When: read() is called
    stream.read();

    // Then: position is advanced
    assertEquals(1, stream.getPos());
  }

  @Test
  void testSeek() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(fakeBlockManager);

    // When
    stream.seek(13);

    // Then
    assertEquals(13, stream.getPos());
  }

  @Test
  void testFullRead() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(fakeBlockManager);

    // When: all data is requested
    String dataReadOut = IoUtils.toUtf8String(stream);

    // Then: data read out is the same as data under stream
    assertEquals(TEST_DATA, dataReadOut);
  }

  @Test
  void testSeekToVeryEnd() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(fakeBlockManager);

    // When: we seek to the last byte
    stream.seek(TEST_DATA.length() - 1);

    // Then: first read returns the last byte and the next read returns -1
    assertEquals(48, stream.read());
    assertEquals(-1, stream.read());
  }

  @Test
  void testSeekAfterEnd() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(fakeBlockManager);

    // When: we seek past EOF we get EOFException
    assertThrows(EOFException.class, () -> stream.seek(TEST_DATA.length() + 1));
  }

  @Test
  void testReadOnEmptyObject() throws IOException {
    // Given
    S3SeekableInputStream stream =
        new S3SeekableInputStream(new BlockManager(new FakeObjectClient(""), TEST_OBJECT));

    // When: we read a byte from the empty object
    int readByte = stream.read();

    // Then: read returns -1
    assertEquals(-1, readByte);
  }

  @Test
  void testInvalidSeek() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(fakeBlockManager);

    // When: seek is to an invalid position then exception is thrown
    assertThrows(Exception.class, () -> stream.seek(TEST_DATA.length()));
    assertThrows(Exception.class, () -> stream.seek(TEST_DATA.length() + 10));
    assertThrows(Exception.class, () -> stream.seek(Long.MAX_VALUE));
    assertThrows(Exception.class, () -> stream.seek(-1));
    assertThrows(Exception.class, () -> stream.seek(Long.MIN_VALUE));
  }

  @Test
  void testBlockManagerGetsClosed() throws IOException {
    // Given
    BlockManager blockManager = mock(BlockManager.class);
    S3SeekableInputStream stream = new S3SeekableInputStream(blockManager);

    // When
    stream.close();

    // Then
    verify(blockManager, times(1)).close();
  }
}
