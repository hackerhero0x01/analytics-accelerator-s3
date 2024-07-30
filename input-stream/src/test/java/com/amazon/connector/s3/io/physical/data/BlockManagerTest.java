package com.amazon.connector.s3.io.physical.data;

import static com.amazon.connector.s3.util.Constants.ONE_KB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.ReadMode;
import com.amazon.connector.s3.util.S3URI;
import java.io.ByteArrayInputStream;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class BlockManagerTest {

  @Test
  void test__getBlock__isEmpty() {
    // Given
    BlockManager blockManager = getTestBlockManager(42);

    // When: nothing

    // Then
    assertFalse(blockManager.getBlock(0).isPresent());
  }

  @Test
  @SneakyThrows
  void test__getBlock__returnsAvailableBlock() {
    // Given
    BlockManager blockManager = getTestBlockManager(65 * ONE_KB);

    // When: have a 64KB block available from 0
    blockManager.makePositionAvailable(0, ReadMode.SYNC);

    // Then: 0 returns a block but 64KB + 1 byte returns no block
    assertTrue(blockManager.getBlock(0).isPresent());
    assertFalse(blockManager.getBlock(64 * ONE_KB).isPresent());
  }

  @Test
  @SneakyThrows
  void test__makePositionAvailable__respectsReadAhead() {
    // Given
    final int objectSize = (int) PhysicalIOConfiguration.DEFAULT.getReadAheadBytes() + ONE_KB;
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, objectSize);

    // When
    blockManager.makePositionAvailable(0, ReadMode.SYNC);

    // Then
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient).getObject(requestCaptor.capture());

    assertEquals(0, requestCaptor.getValue().getRange().getStart());
    assertEquals(
        PhysicalIOConfiguration.DEFAULT.getReadAheadBytes() - 1,
        requestCaptor.getValue().getRange().getEnd());
  }

  @Test
  @SneakyThrows
  void test__makePositionAvailable__respectsLastObjectByte() {
    // Given
    final int objectSize = 5 * ONE_KB;
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, objectSize);

    // When
    blockManager.makePositionAvailable(0, ReadMode.SYNC);

    // Then
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient).getObject(requestCaptor.capture());

    assertEquals(0, requestCaptor.getValue().getRange().getStart());
    assertEquals(objectSize - 1, requestCaptor.getValue().getRange().getEnd());
  }

  @Test
  @SneakyThrows
  void test__makeRangeAvailable__doesNotOverread() {
    // Given: BM with 0-64KB and 64KB+1 to 128KB
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager = getTestBlockManager(objectClient, 128 * ONE_KB);
    blockManager.makePositionAvailable(0, ReadMode.SYNC);
    blockManager.makePositionAvailable(64 * ONE_KB + 1, ReadMode.SYNC);

    // When: requesting the byte at 64KB
    blockManager.makeRangeAvailable(64 * ONE_KB, 100, ReadMode.SYNC);
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient, times(3)).getObject(requestCaptor.capture());

    // Then: request size is a single byte as more is not needed
    GetRequest firstRequest = requestCaptor.getAllValues().get(0);
    GetRequest secondRequest = requestCaptor.getAllValues().get(1);
    GetRequest lastRequest = requestCaptor.getAllValues().get(2);

    assertEquals(65_536, firstRequest.getRange().getSize());
    assertEquals(65_535, secondRequest.getRange().getSize());
    assertEquals(1, lastRequest.getRange().getSize());
  }

  private BlockManager getTestBlockManager(int size) {
    return getTestBlockManager(mock(ObjectClient.class), size);
  }

  private BlockManager getTestBlockManager(ObjectClient objectClient, int size) {
    S3URI testUri = S3URI.of("foo", "bar");

    when(objectClient.headObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectMetadata.builder().contentLength(size).build()));
    when(objectClient.getObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ByteArrayInputStream(new byte[size])).build()));

    MetadataStore metadataStore = new MetadataStore(objectClient, PhysicalIOConfiguration.DEFAULT);

    return new BlockManager(testUri, objectClient, metadataStore, PhysicalIOConfiguration.DEFAULT);
  }
}
