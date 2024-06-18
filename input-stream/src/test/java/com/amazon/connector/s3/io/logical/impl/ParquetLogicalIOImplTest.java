package com.amazon.connector.s3.io.logical.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManager;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManagerConfiguration;
import com.amazon.connector.s3.io.physical.impl.PhysicalIOImpl;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class ParquetLogicalIOImplTest {

  @Test
  void testContructor() {
    assertNotNull(
        new ParquetLogicalIOImpl(
            mock(PhysicalIO.class),
            LogicalIOConfiguration.builder().footerCachingEnabled(false).build()));
  }

  @Test
  void testCloseDependencies() throws IOException {
    // Given
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetLogicalIOImpl logicalIO =
        new ParquetLogicalIOImpl(
            physicalIO, LogicalIOConfiguration.builder().footerCachingEnabled(false).build());

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

  //  @Test
  //  void testFooterPrefetchedAndMetadateParsed() throws IOException {
  //    PhysicalIO physicalIO = mock(PhysicalIO.class);
  //    ParquetMetadataTask parquetMetadataTask = mock(ParquetMetadataTask.class);
  //    ParquetReadTailTask parquetReadTailTask = mock(ParquetReadTailTask.class);
  //    ParquetPrefetchTailTask parquetPrefetchTailTask = mock(ParquetPrefetchTailTask.class);
  //    ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask =
  //        mock(ParquetPrefetchRemainingColumnTask.class);
  //
  //    when(parquetReadTailTask.get()).thenReturn(new FileTail(ByteBuffer.allocate(0), 0));
  //
  //    ParquetLogicalIOImpl logicalIO =
  //        new ParquetLogicalIOImpl(
  //            physicalIO,
  //            LogicalIOConfiguration.DEFAULT,
  //            parquetPrefetchTailTask,
  //            parquetReadTailTask,
  //            parquetMetadataTask,
  //            parquetPrefetchRemainingColumnTask);
  //
  //    logicalIO.read(new byte[0], 0, 0, 0);
  //
  //    verify(parquetPrefetchRemainingColumnTask).prefetchRemainingColumnChunk(anyLong(),
  // anyInt());
  //  }

  //  @Test
  //  void testFooterPrefetchedAndMetadateParsed() {
  //    PhysicalIO physicalIO = mock(PhysicalIO.class);
  //    ParquetMetadataTask parquetMetadataTask = mock(ParquetMetadataTask.class);
  //    ParquetReadTailTask parquetReadTailTask = mock(ParquetReadTailTask.class);
  //    ParquetPrefetchTailTask parquetPrefetchTailTask = mock(ParquetPrefetchTailTask.class);
  //
  //    when(physicalIO.columnMappers()).thenReturn(null);
  //    when(parquetReadTailTask.get()).thenReturn(new FileTail(ByteBuffer.allocate(0), 0));
  //
  //    new ParquetLogicalIOImpl(
  //        physicalIO,
  //        LogicalIOConfiguration.DEFAULT,
  //        parquetPrefetchTailTask,
  //        parquetReadTailTask,
  //        parquetMetadataTask);
  //
  //    verify(parquetPrefetchTailTask).prefetchTail();
  //    verify(parquetReadTailTask).get();
  //    verify(parquetMetadataTask).storeColumnMappers(any(FileTail.class));
  //  }
}
