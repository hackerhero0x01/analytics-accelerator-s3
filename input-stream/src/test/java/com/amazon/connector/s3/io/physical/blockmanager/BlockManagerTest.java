package com.amazon.connector.s3.io.physical.blockmanager;

import static com.amazon.connector.s3.util.Constants.ONE_MB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.util.S3URI;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class BlockManagerTest {

  private static final S3URI URI = S3URI.of("bucket", "key");

  @Test
  void testConstructor() {
    // When: constructor is called
    BlockManager blockManager =
        new BlockManager(mock(ObjectClient.class), URI, BlockManagerConfiguration.DEFAULT);

    // Then: result is not null
    assertNotNull(blockManager);
  }

  @Test
  void testConstructorFailsOnNull() {
    assertThrows(
        NullPointerException.class,
        () -> new BlockManager(null, URI, BlockManagerConfiguration.DEFAULT));
    assertThrows(
        NullPointerException.class,
        () -> new BlockManager(mock(ObjectClient.class), null, BlockManagerConfiguration.DEFAULT));
    assertThrows(
        NullPointerException.class, () -> new BlockManager(mock(ObjectClient.class), URI, null));
  }

  @Test
  void testClose() throws IOException {
    // Given: object client
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager =
        new BlockManager(objectClient, URI, BlockManagerConfiguration.DEFAULT);

    // When: close is called
    blockManager.close();

    // Object client is not closed, as we want to share the client b/w streams.
    verify(objectClient, times(0)).close();
  }

  @Test
  void testBlockManager_usesMetadata() throws IOException {
    // Given: block manager
    int contentLength = ONE_MB;
    byte[] content = new byte[contentLength];

    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.getObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ByteArrayInputStream(content)).build()));
    when(objectClient.headObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectMetadata.builder().contentLength(contentLength).build()));
    BlockManager blockManager =
        new BlockManager(objectClient, URI, BlockManagerConfiguration.DEFAULT);

    // When: data asked for is more than object size
    byte[] buf = new byte[16 * ONE_MB];
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    blockManager.read(buf, 0, buf.length, 0);

    // Then: only object size is requested
    verify(objectClient).getObject(requestCaptor.capture());
    GetRequest getRequest = requestCaptor.getValue();
    assertEquals(0L, getRequest.getRange().getStart());
    assertEquals(ONE_MB - 1, getRequest.getRange().getEnd());
  }

  @Test
  void testBlockManager_usesReadAheadConfig() throws IOException {
    // Given: block manager
    int contentLength = ONE_MB;
    int readAheadConfig = 123;
    byte[] content = new byte[contentLength];

    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.getObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ByteArrayInputStream(content)).build()));
    when(objectClient.headObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectMetadata.builder().contentLength(contentLength).build()));
    BlockManager blockManager =
        new BlockManager(
            objectClient,
            URI,
            BlockManagerConfiguration.builder().readAheadBytes(readAheadConfig).build());

    // When: data asked for is less than read ahead config
    byte[] buf = new byte[16];
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    blockManager.read(buf, 0, buf.length, 0);

    // Then: only object size is requested
    verify(objectClient).getObject(requestCaptor.capture());
    GetRequest getRequest = requestCaptor.getValue();
    assertEquals(0L, getRequest.getRange().getStart());
    assertEquals(readAheadConfig - 1, getRequest.getRange().getEnd());
  }

  @Test
  // TODO: This test should be modified at some point to test for retries when we introduce them:
  //  ticket: https://app.asana.com/0/1206885953994785/1207374694729991/f
  void testBlockManager_doesNotRetry() throws IOException {
    // Given: block manager AND S3 is unavailable
    int contentLength = ONE_MB;
    int readAheadConfig = 123;

    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.getObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ThrowingInputStream()).build()));
    when(objectClient.headObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectMetadata.builder().contentLength(contentLength).build()));
    BlockManager blockManager =
        new BlockManager(
            objectClient,
            URI,
            BlockManagerConfiguration.builder().readAheadBytes(readAheadConfig).build());

    // Then: block manager throws IOException
    byte[] buf = new byte[16];
    assertThrows(IOException.class, () -> blockManager.read(buf, 0, buf.length, 0));
  }

  private static class ThrowingInputStream extends InputStream {
    @Override
    public int read() throws IOException {
      throw new IOException("I always throw!");
    }
  }
}
