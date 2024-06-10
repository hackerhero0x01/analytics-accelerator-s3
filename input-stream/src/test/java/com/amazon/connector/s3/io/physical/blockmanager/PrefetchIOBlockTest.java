package com.amazon.connector.s3.io.physical.blockmanager;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.connector.s3.object.ObjectContent;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class PrefetchIOBlockTest {
  @Test
  void testConstructor() throws IOException {
    InputStream mockStream = mock(InputStream.class);
    CompletableFuture<ObjectContent> mockContent =
        CompletableFuture.completedFuture(ObjectContent.builder().stream(mockStream).build());
    CompletableFuture<IOBlock> ioBlockCompletableFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return new IOBlock(0, 0, mockContent);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });

    assertNotNull(new PrefetchIOBlock(0, 0, ioBlockCompletableFuture));
    ioBlockCompletableFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return new IOBlock(0, Long.MAX_VALUE, mockContent);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    assertNotNull(new PrefetchIOBlock(0, Long.MAX_VALUE, ioBlockCompletableFuture));
    ioBlockCompletableFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return new IOBlock(10, 20, mockContent);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    assertNotNull(new PrefetchIOBlock(10, 20, ioBlockCompletableFuture));
  }

  @Test
  void testClose() throws IOException {
    // Given
    InputStream mockStream = mock(InputStream.class);
    CompletableFuture<IOBlock> ioBlockCompletableFuture =
        CompletableFuture.completedFuture(
            new IOBlock(
                0,
                100,
                CompletableFuture.completedFuture(
                    ObjectContent.builder().stream(mockStream).build())));

    PrefetchIOBlock prefetchIOBlock = new PrefetchIOBlock(0, 100, ioBlockCompletableFuture);

    // When
    prefetchIOBlock.close();
    // Then: stream is closed
    verify(mockStream, times(2)).close();
  }

  @Test
  void testConstructorThrows() throws IOException {
    CompletableFuture<ObjectContent> mockContent =
        CompletableFuture.completedFuture(mock(ObjectContent.class));

    CompletableFuture<IOBlock> cf =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return new IOBlock(-1, 100, mockContent);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    PrefetchIOBlock prefetchIOBlock = new PrefetchIOBlock(-1, 100, cf);
    assertFalse(prefetchIOBlock.getIOBlock().isPresent());
  }
}
