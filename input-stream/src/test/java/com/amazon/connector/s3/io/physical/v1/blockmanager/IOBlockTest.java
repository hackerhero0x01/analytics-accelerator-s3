package com.amazon.connector.s3.io.physical.v1.blockmanager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.connector.s3.object.ObjectContent;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.utils.StringUtils;

public class IOBlockTest {

  @Test
  void testConstructor() throws IOException {
    InputStream mockStream = mock(InputStream.class);
    CompletableFuture<ObjectContent> mockContent =
        CompletableFuture.completedFuture(ObjectContent.builder().stream(mockStream).build());

    assertNotNull(new IOBlock(0, 0, mockContent));
    assertNotNull(new IOBlock(0, Long.MAX_VALUE, mockContent));
    assertNotNull(new IOBlock(10, 20, mockContent));
  }

  @Test
  void testConstructorThrows() {
    CompletableFuture<ObjectContent> mockContent =
        CompletableFuture.completedFuture(mock(ObjectContent.class));

    assertThrows(Exception.class, () -> new IOBlock(-1, 100, mockContent));
    assertThrows(Exception.class, () -> new IOBlock(100, -200, mockContent));
    assertThrows(Exception.class, () -> new IOBlock(200, 100, mockContent));
    assertThrows(Exception.class, () -> new IOBlock(100, 200, null));
  }

  @Test
  void testClose() throws IOException {
    // Given
    InputStream mockStream = mock(InputStream.class);
    IOBlock ioBlock =
        new IOBlock(
            0,
            100,
            CompletableFuture.completedFuture(ObjectContent.builder().stream(mockStream).build()));

    // When: ioBlock closed
    ioBlock.close();

    // Then: stream is closed
    verify(mockStream, times(2)).close();
  }

  @Test
  void testContains() throws IOException {
    // Given
    InputStream mockStream = mock(InputStream.class);
    CompletableFuture<ObjectContent> mockContent =
        CompletableFuture.completedFuture(ObjectContent.builder().stream(mockStream).build());
    IOBlock ioBlock = new IOBlock(1, 3, mockContent);

    // Then
    assertFalse(ioBlock.contains(Long.MIN_VALUE));
    assertFalse(ioBlock.contains(0));
    assertTrue(ioBlock.contains(1));
    assertTrue(ioBlock.contains(2));
    assertTrue(ioBlock.contains(3));
    assertFalse(ioBlock.contains(4));
    assertFalse(ioBlock.contains(Long.MAX_VALUE));
  }

  @Test
  void testPrematureClose() {
    // Given: stream not spanning the whole range the IOBlock represents
    int streamLength = 10;
    ObjectContent content =
        ObjectContent.builder().stream(new ByteArrayInputStream(new byte[streamLength])).build();
    int ioBlockLength = 2 * streamLength;

    Exception e =
        assertThrows(
            IOException.class,
            () -> new IOBlock(0, ioBlockLength, CompletableFuture.completedFuture(content)));

    assertTrue(e.getMessage().contains("Unexpected end of stream"));
  }

  @Test
  void testGetByte() throws IOException {
    // Given 100 bytes stream, with "1" at 0, 49 and 99 indexes and IOBlock was created with offset
    StringBuilder sb = new StringBuilder(100);
    sb.append(StringUtils.repeat("0", 100));
    sb.replace(0, 0, "1");
    sb.replace(49, 49, "1");
    sb.replace(99, 99, "1");
    InputStream mockStream = new ByteArrayInputStream(sb.toString().getBytes());
    CompletableFuture<ObjectContent> mockContent =
        CompletableFuture.completedFuture(ObjectContent.builder().stream(mockStream).build());
    int offset = 10;
    IOBlock ioBlock = new IOBlock(0 + offset, 99 + offset, mockContent);

    int one = '1';
    int zero = '0';
    // When reading offset + any of (0, 49 and 99) bytes, we should see `1`
    // When reading neighbours bytes, we should see `0`
    assertEquals(one, ioBlock.getByte(0 + offset));
    assertEquals(one, ioBlock.getByte(49 + offset));
    assertEquals(one, ioBlock.getByte(99 + offset));
    assertEquals(zero, ioBlock.getByte(1 + offset));
    assertEquals(zero, ioBlock.getByte(48 + offset));
    assertEquals(zero, ioBlock.getByte(50 + offset));
    assertEquals(zero, ioBlock.getByte(98 + offset));

    // When asking for bytes that is outside of range, we should get exception
    assertThrows(IllegalStateException.class, () -> ioBlock.getByte(101 + offset));
    assertThrows(IllegalStateException.class, () -> ioBlock.getByte(-1 + offset));
  }
}
