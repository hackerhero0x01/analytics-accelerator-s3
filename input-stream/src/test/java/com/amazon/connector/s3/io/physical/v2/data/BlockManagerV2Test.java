package com.amazon.connector.s3.io.physical.v2.data;

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
import com.amazon.connector.s3.io.physical.v1.blockmanager.BlockManagerConfiguration;
import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.util.S3URI;
import java.io.ByteArrayInputStream;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class BlockManagerV2Test {

  @Test
  void test__getBlock__isEmpty() {
    // Given
    BlockManagerV2 blockManagerV2 = getTestBlockManager(42);

    // When: nothing

    // Then
    assertFalse(blockManagerV2.getBlock(0).isPresent());
  }

  @Test
  void test__getBlock__returnsAvailableBlock() {
    // Given
    BlockManagerV2 blockManagerV2 = getTestBlockManager(65 * ONE_KB);

    // When: have a 64KB block available from 0
    blockManagerV2.makePositionAvailable(0);

    // Then: 0 returns a block but 64KB + 1 byte returns no block
    assertTrue(blockManagerV2.getBlock(0).isPresent());
    assertFalse(blockManagerV2.getBlock(64 * ONE_KB).isPresent());
  }

  @Test
  void test__makePositionAvailable__respectsReadAhead() {
    // Given
    final int objectSize = (int) BlockManagerConfiguration.DEFAULT.getReadAheadBytes() + ONE_KB;
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManagerV2 blockManagerV2 = getTestBlockManager(objectClient, objectSize);

    // When
    blockManagerV2.makePositionAvailable(0);

    // Then
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient).getObject(requestCaptor.capture());

    assertEquals(0, requestCaptor.getValue().getRange().getStart());
    assertEquals(
        BlockManagerConfiguration.DEFAULT.getReadAheadBytes() - 1,
        requestCaptor.getValue().getRange().getEnd());
  }

  @Test
  void test__makePositionAvailable__respectsLastObjectByte() {
    // Given
    final int objectSize = 5 * ONE_KB;
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManagerV2 blockManagerV2 = getTestBlockManager(objectClient, objectSize);

    // When
    blockManagerV2.makePositionAvailable(0);

    // Then
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    verify(objectClient).getObject(requestCaptor.capture());

    assertEquals(0, requestCaptor.getValue().getRange().getStart());
    assertEquals(objectSize - 1, requestCaptor.getValue().getRange().getEnd());
  }

  @Test
  void test__makeRangeAvailable__doesNotOverread() {
    // Given: BM with 0-64KB and 64KB+1 to 128KB
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManagerV2 blockManagerV2 = getTestBlockManager(objectClient, 128 * ONE_KB);
    blockManagerV2.makePositionAvailable(0);
    blockManagerV2.makePositionAvailable(64 * ONE_KB + 1);

    // When: requesting the byte at 64KB
    blockManagerV2.makeRangeAvailable(64 * ONE_KB, 100);
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

  private BlockManagerV2 getTestBlockManager(int size) {
    return getTestBlockManager(mock(ObjectClient.class), size);
  }

  private BlockManagerV2 getTestBlockManager(ObjectClient objectClient, int size) {
    S3URI testUri = S3URI.of("foo", "bar");
    when(objectClient.getObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ByteArrayInputStream(new byte[size])).build()));

    MetadataStore metadataStore = mock(MetadataStore.class);
    when(metadataStore.get(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectMetadata.builder().contentLength(size).build()));
    return new BlockManagerV2(testUri, objectClient, metadataStore);
  }
}
