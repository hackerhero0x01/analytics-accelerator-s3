package com.amazon.connector.s3.io.logical.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.impl.PhysicalIOImpl;
import com.amazon.connector.s3.io.physical.v1.blockmanager.BlockManager;
import com.amazon.connector.s3.io.physical.v1.blockmanager.BlockManagerConfiguration;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class ParquetLogicalIOImplTest {

  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  void testConstructor() {
    assertNotNull(
        new ParquetLogicalIOImpl(
            S3URI.of("foo", "bar"),
            mock(PhysicalIO.class),
            mock(LogicalIOConfiguration.class),
            mock(ParquetMetadataStore.class)));
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                null,
                mock(LogicalIOConfiguration.class),
                mock(ParquetMetadataStore.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI, mock(PhysicalIO.class), null, mock(ParquetMetadataStore.class)));
  }

  @Test
  void testCloseDependencies() throws IOException {
    // Given
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration configuration =
        LogicalIOConfiguration.builder()
            .footerCachingEnabled(false)
            .metadataAwarePrefetchingEnabled(false)
            .build();

    ParquetLogicalIOImpl logicalIO =
        new ParquetLogicalIOImpl(
            TEST_URI, physicalIO, configuration, new ParquetMetadataStore(configuration));

    // When: close called
    logicalIO.close();

    // Then: close will close dependencies
    verify(physicalIO, times(1)).close();
  }

  @Test
  void testMetadaWithZeroContentLength() {
    ObjectClient mockClient = mock(ObjectClient.class);
    when(mockClient.headObject(any(HeadRequest.class)))
        .thenReturn(
            CompletableFuture.completedFuture(ObjectMetadata.builder().contentLength(0).build()));
    S3URI s3URI = S3URI.of("test", "test");
    BlockManager blockManager =
        new BlockManager(mockClient, s3URI, BlockManagerConfiguration.DEFAULT);
    PhysicalIOImpl physicalIO = new PhysicalIOImpl(blockManager);
    assertDoesNotThrow(
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                physicalIO,
                LogicalIOConfiguration.DEFAULT,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT)));
  }

  @Test
  void testMetadaWithNegativeContentLength() {
    ObjectClient mockClient = mock(ObjectClient.class);
    when(mockClient.headObject(any(HeadRequest.class)))
        .thenReturn(
            CompletableFuture.completedFuture(ObjectMetadata.builder().contentLength(-1).build()));
    S3URI s3URI = S3URI.of("test", "test");
    BlockManager blockManager =
        new BlockManager(mockClient, s3URI, BlockManagerConfiguration.DEFAULT);
    PhysicalIOImpl physicalIO = new PhysicalIOImpl(blockManager);
    assertDoesNotThrow(
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                physicalIO,
                LogicalIOConfiguration.DEFAULT,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT)));
  }
}
