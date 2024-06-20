package com.amazon.connector.s3.io.logical.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.io.logical.parquet.ParquetMetadataTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetPredictivePrefetchingTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetPrefetchRemainingColumnTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetPrefetchTailTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetReadTailTask;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManager;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManagerConfiguration;
import com.amazon.connector.s3.io.physical.impl.PhysicalIOImpl;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class ParquetLogicalIOImplTest {

  @Test
  void testContructor() {
    assertNotNull(
        new ParquetLogicalIOImpl(
            mock(PhysicalIO.class),
            LogicalIOConfiguration.builder()
                .footerCachingEnabled(false)
                .metadataAwarePefetchingEnabled(false)
                .build()));
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () -> new ParquetLogicalIOImpl(null, LogicalIOConfiguration.DEFAULT));
    assertThrows(
        NullPointerException.class, () -> new ParquetLogicalIOImpl(mock(PhysicalIO.class), null));
  }

  @Test
  void testCloseDependencies() throws IOException {
    // Given
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetLogicalIOImpl logicalIO =
        new ParquetLogicalIOImpl(
            physicalIO,
            LogicalIOConfiguration.builder()
                .footerCachingEnabled(false)
                .metadataAwarePefetchingEnabled(false)
                .build());

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
    assertDoesNotThrow(() -> new ParquetLogicalIOImpl(physicalIO, LogicalIOConfiguration.DEFAULT));
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
    assertDoesNotThrow(() -> new ParquetLogicalIOImpl(physicalIO, LogicalIOConfiguration.DEFAULT));
  }

  @Test
  void testRemainingColumnPrefetched() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetLogicalIOImpl logicalIO =
        getMockedLogicalIO(
            physicalIO,
            LogicalIOConfiguration.builder().predictivePrefetchingEnabled(false).build());
    assertEquals(true, logicalIO.prefetchRemainingColumnChunk(0, 0).isPresent());
  }

  @Test
  void testFooterPrefetchedAndMetadataParsed() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetLogicalIOImpl logicalIO = getMockedLogicalIO(physicalIO, LogicalIOConfiguration.DEFAULT);
    assertEquals(true, logicalIO.prefetchFooterAndBuildMetadata().isPresent());
  }

  @Test
  void testMetadataAwarePrefetchingDisabled() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetLogicalIOImpl logicalIO =
        getMockedLogicalIO(
            physicalIO,
            LogicalIOConfiguration.builder().metadataAwarePefetchingEnabled(false).build());
    assertEquals(logicalIO.prefetchFooterAndBuildMetadata().isPresent(), true);
    assertEquals(logicalIO.prefetchRemainingColumnChunk(0, 0).isPresent(), false);
  }

  @Test
  void testNoPrefetchingWhenColumnMappersExist() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    when(physicalIO.columnMappers())
        .thenReturn(new ColumnMappers(new HashMap<>(), new HashMap<>()));
    ParquetLogicalIOImpl logicalIO = getMockedLogicalIO(physicalIO, LogicalIOConfiguration.DEFAULT);
    assertFalse(logicalIO.prefetchFooterAndBuildMetadata().isPresent());
  }

  @Test
  void testNoPredictivePrefetchingWhenDisabled() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);

    ParquetLogicalIOImpl logicalIO =
        getMockedLogicalIO(
            physicalIO,
            LogicalIOConfiguration.builder().predictivePrefetchingEnabled(false).build());

    CompletableFuture<Optional<ColumnMappers>> optionalCompletableFuture =
        CompletableFuture.completedFuture(
            Optional.of(new ColumnMappers(new HashMap<>(), new HashMap<>())));

    assertEquals(logicalIO.prefetchFooterAndBuildMetadata().isPresent(), true);
    assertEquals(logicalIO.prefetchRemainingColumnChunk(0, 0).isPresent(), true);
    assertFalse(logicalIO.prefetchPredictedColumns(optionalCompletableFuture).isPresent());
  }

  private ParquetLogicalIOImpl getMockedLogicalIO(
      PhysicalIO physicalIO, LogicalIOConfiguration logicalIOConfiguration) {
    ParquetMetadataTask parquetMetadataTask = mock(ParquetMetadataTask.class);
    ParquetReadTailTask parquetReadTailTask = mock(ParquetReadTailTask.class);
    ParquetPrefetchTailTask parquetPrefetchTailTask = mock(ParquetPrefetchTailTask.class);
    ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask =
        mock(ParquetPrefetchRemainingColumnTask.class);
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        mock(ParquetPredictivePrefetchingTask.class);

    return new ParquetLogicalIOImpl(
        physicalIO,
        logicalIOConfiguration,
        parquetPrefetchTailTask,
        parquetReadTailTask,
        parquetMetadataTask,
        parquetPrefetchRemainingColumnTask,
        parquetPredictivePrefetchingTask);
  }
}
