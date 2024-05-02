package com.amazon.connector.s3.blockmanager;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.amazon.connector.s3.object.ObjectContent2;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class IOBlockTest {

  @Test
  void testConstructor() {
    CompletableFuture<ObjectContent2> mockContent =
        CompletableFuture.completedFuture(mock(ObjectContent2.class));

    assertNotNull(new IOBlock(0, 0, mockContent));
    assertNotNull(new IOBlock(0, Long.MAX_VALUE, mockContent));
    assertNotNull(new IOBlock(10, 20, mockContent));
  }

  @Test
  void testConstructorThrows() {
    CompletableFuture<ObjectContent2> mockContent =
        CompletableFuture.completedFuture(mock(ObjectContent2.class));

    assertThrows(Exception.class, () -> new IOBlock(-1, 100, mockContent));
    assertThrows(Exception.class, () -> new IOBlock(100, -200, mockContent));
    assertThrows(Exception.class, () -> new IOBlock(200, 100, mockContent));
    assertThrows(Exception.class, () -> new IOBlock(100, 200, null));
  }

  @Test
  void testContains() {
    // Given
    CompletableFuture<ObjectContent2> mockContent =
        CompletableFuture.completedFuture(mock(ObjectContent2.class));
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
}
