package com.amazon.connector.s3.io.logical.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.io.logical.parquet.FileTail;
import com.amazon.connector.s3.io.logical.parquet.ParquetMetadataParsingTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetPredictivePrefetchingTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetPrefetchRemainingColumnTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetPrefetchTailTask;
import com.amazon.connector.s3.io.logical.parquet.ParquetReadTailTask;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlanExecution;
import com.amazon.connector.s3.io.physical.plan.IOPlanState;
import com.amazon.connector.s3.util.S3URI;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class ParquetPrefetcherTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  public void testConstructor() {
    assertNotNull(
        new ParquetPrefetcher(
            mock(S3URI.class),
            mock(PhysicalIO.class),
            mock(Telemetry.class),
            mock(LogicalIOConfiguration.class),
            mock(ParquetColumnPrefetchStore.class)));
  }

  @Test
  public void testConstructorNulls() {
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                null,
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(Telemetry.class),
                mock(ParquetMetadataParsingTask.class),
                mock(ParquetPrefetchTailTask.class),
                mock(ParquetReadTailTask.class),
                mock(ParquetPrefetchRemainingColumnTask.class),
                mock(ParquetPredictivePrefetchingTask.class)));

    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                null,
                mock(ParquetColumnPrefetchStore.class),
                mock(Telemetry.class),
                mock(ParquetMetadataParsingTask.class),
                mock(ParquetPrefetchTailTask.class),
                mock(ParquetReadTailTask.class),
                mock(ParquetPrefetchRemainingColumnTask.class),
                mock(ParquetPredictivePrefetchingTask.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(LogicalIOConfiguration.class),
                null,
                mock(Telemetry.class),
                mock(ParquetMetadataParsingTask.class),
                mock(ParquetPrefetchTailTask.class),
                mock(ParquetReadTailTask.class),
                mock(ParquetPrefetchRemainingColumnTask.class),
                mock(ParquetPredictivePrefetchingTask.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                null,
                mock(ParquetMetadataParsingTask.class),
                mock(ParquetPrefetchTailTask.class),
                mock(ParquetReadTailTask.class),
                mock(ParquetPrefetchRemainingColumnTask.class),
                mock(ParquetPredictivePrefetchingTask.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(Telemetry.class),
                null,
                mock(ParquetPrefetchTailTask.class),
                mock(ParquetReadTailTask.class),
                mock(ParquetPrefetchRemainingColumnTask.class),
                mock(ParquetPredictivePrefetchingTask.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(Telemetry.class),
                mock(ParquetMetadataParsingTask.class),
                null,
                mock(ParquetReadTailTask.class),
                mock(ParquetPrefetchRemainingColumnTask.class),
                mock(ParquetPredictivePrefetchingTask.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(Telemetry.class),
                mock(ParquetMetadataParsingTask.class),
                mock(ParquetPrefetchTailTask.class),
                null,
                mock(ParquetPrefetchRemainingColumnTask.class),
                mock(ParquetPredictivePrefetchingTask.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(Telemetry.class),
                mock(ParquetMetadataParsingTask.class),
                mock(ParquetPrefetchTailTask.class),
                mock(ParquetReadTailTask.class),
                null,
                mock(ParquetPredictivePrefetchingTask.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class),
                mock(Telemetry.class),
                mock(ParquetMetadataParsingTask.class),
                mock(ParquetPrefetchTailTask.class),
                mock(ParquetReadTailTask.class),
                mock(ParquetPrefetchRemainingColumnTask.class),
                null));

    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                null,
                mock(PhysicalIO.class),
                mock(Telemetry.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                null,
                mock(Telemetry.class),
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(PhysicalIO.class),
                null,
                mock(LogicalIOConfiguration.class),
                mock(ParquetColumnPrefetchStore.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(PhysicalIO.class),
                mock(Telemetry.class),
                null,
                mock(ParquetColumnPrefetchStore.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetcher(
                mock(S3URI.class),
                mock(PhysicalIO.class),
                mock(Telemetry.class),
                mock(LogicalIOConfiguration.class),
                null));
  }

  @Test
  public void testConfigurationsPrefetchRemainingColumnChunkEnabled() {
    // Given: config with metadata awareness ENABLED but predictive prefetching DISABLED
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.builder()
            .metadataAwarePrefetchingEnabled(true)
            .predictivePrefetchingEnabled(false)
            .build();

    ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask =
        mock(ParquetPrefetchRemainingColumnTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            mock(ParquetMetadataParsingTask.class),
            mock(ParquetPrefetchTailTask.class),
            mock(ParquetReadTailTask.class),
            parquetPrefetchRemainingColumnTask,
            mock(ParquetPredictivePrefetchingTask.class));

    // When: prefetching a column chunk
    parquetPrefetcher.prefetchRemainingColumnChunk(100, 200).join();

    // Then: remaining columns are requested
    verify(parquetPrefetchRemainingColumnTask, times(1))
        .prefetchRemainingColumnChunk(anyLong(), anyInt());
  }

  @ParameterizedTest
  @MethodSource("prefetchRemainingColumnChunkShouldBeSkipped")
  public void testConfigurationsPrefetchRemainingColumnChunkDisabled(
      boolean metadataAwarePrefetching, boolean predictivePrefetching) {
    // Given: config that should not trigger prefetching
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.builder()
            .metadataAwarePrefetchingEnabled(metadataAwarePrefetching)
            .predictivePrefetchingEnabled(predictivePrefetching)
            .build();

    ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask =
        mock(ParquetPrefetchRemainingColumnTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            mock(ParquetMetadataParsingTask.class),
            mock(ParquetPrefetchTailTask.class),
            mock(ParquetReadTailTask.class),
            parquetPrefetchRemainingColumnTask,
            mock(ParquetPredictivePrefetchingTask.class));

    // When: prefetching a column chunk
    IOPlanExecution execution = parquetPrefetcher.prefetchRemainingColumnChunk(100, 200).join();

    // Then: verify that the prefetching task is never actually triggered
    verifyNoInteractions(parquetPrefetchRemainingColumnTask);
    assertEquals(IOPlanState.SKIPPED, execution.getState());
  }

  @Test
  public void testConfigurationsPrefetchFooterAndBuildMetadataDefaultConfig() {
    // Given: default LogicalIO configuration
    LogicalIOConfiguration logicalIOConfiguration = LogicalIOConfiguration.DEFAULT;

    ParquetPrefetchTailTask parquetPrefetchTailTask = mock(ParquetPrefetchTailTask.class);
    ParquetReadTailTask parquetReadTailTask = getTestParquetReadTailTask();
    ParquetMetadataParsingTask parquetMetadataParsingTask = getTestParquetMetadataTask();
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        mock(ParquetPredictivePrefetchingTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            parquetMetadataParsingTask,
            parquetPrefetchTailTask,
            parquetReadTailTask,
            mock(ParquetPrefetchRemainingColumnTask.class),
            parquetPredictivePrefetchingTask);

    // When: footer prefetching and metadata build is requested
    parquetPrefetcher.prefetchFooterAndBuildMetadata().join();

    // Then: tail prefetching is triggered
    verify(parquetPrefetchTailTask, times(1)).prefetchTail();
    // Then: read tail is triggered
    verify(parquetReadTailTask, times(1)).readFileTail();
    // Then: columns are stored
    verify(parquetMetadataParsingTask, times(1)).storeColumnMappers(any(FileTail.class));
    // Then: predictive reads are also triggered
    verify(parquetPredictivePrefetchingTask, times(1))
        .prefetchRecentColumns(any(ColumnMappers.class));
  }

  @Test
  public void testConfigurationsPrefetchFooterAndBuildMetadataNoPredictivePrefetching() {
    // Given: config with predictive prefetching disabled
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.builder().predictivePrefetchingEnabled(false).build();

    ParquetPrefetchTailTask parquetPrefetchTailTask = mock(ParquetPrefetchTailTask.class);
    ParquetReadTailTask parquetReadTailTask = getTestParquetReadTailTask();
    ParquetMetadataParsingTask parquetMetadataParsingTask = getTestParquetMetadataTask();
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        mock(ParquetPredictivePrefetchingTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            parquetMetadataParsingTask,
            parquetPrefetchTailTask,
            parquetReadTailTask,
            mock(ParquetPrefetchRemainingColumnTask.class),
            parquetPredictivePrefetchingTask);

    // When: footer prefetching and metadata build is requested
    parquetPrefetcher.prefetchFooterAndBuildMetadata().join();

    // Then: tail prefetching is still triggered
    verify(parquetPrefetchTailTask, times(1)).prefetchTail();
    // Then: BUT predictive reads are NOT triggered
    verifyNoInteractions(parquetPredictivePrefetchingTask);
  }

  @Test
  public void testConfigurationsPrefetchFooterAndBuildMetadataAllPrefetchDisabled() {
    // Given: config with all prefetching disabled
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.builder()
            .predictivePrefetchingEnabled(false)
            .metadataAwarePrefetchingEnabled(false)
            .build();

    ParquetPrefetchTailTask parquetPrefetchTailTask = mock(ParquetPrefetchTailTask.class);
    ParquetReadTailTask parquetReadTailTask = getTestParquetReadTailTask();
    ParquetMetadataParsingTask parquetMetadataParsingTask = getTestParquetMetadataTask();
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        mock(ParquetPredictivePrefetchingTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            parquetMetadataParsingTask,
            parquetPrefetchTailTask,
            parquetReadTailTask,
            mock(ParquetPrefetchRemainingColumnTask.class),
            parquetPredictivePrefetchingTask);

    // When: footer prefetching and metadata build is requested
    parquetPrefetcher.prefetchFooterAndBuildMetadata().join();

    // Then: tail prefetching is triggered
    verify(parquetPrefetchTailTask, times(1)).prefetchTail();
    // Then: read tail is NOT triggered
    verifyNoInteractions(parquetReadTailTask);
    // Then: columns are NOT parsed and stored
    verifyNoInteractions(parquetMetadataParsingTask);
    // Then: predictive reads are NOT triggered
    verifyNoInteractions(parquetPredictivePrefetchingTask);
  }

  @Test
  public void testConfigurationsPrefetchFooterAndBuildMetadataFooterCachingDisabled() {
    // Given: config with footer caching disabled
    LogicalIOConfiguration logicalIOConfiguration =
        LogicalIOConfiguration.builder().footerCachingEnabled(false).build();

    ParquetPrefetchTailTask parquetPrefetchTailTask = mock(ParquetPrefetchTailTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            mock(ParquetMetadataParsingTask.class),
            parquetPrefetchTailTask,
            mock(ParquetReadTailTask.class),
            mock(ParquetPrefetchRemainingColumnTask.class),
            mock(ParquetPredictivePrefetchingTask.class));

    // When: footer prefetching and metadata build is requested
    parquetPrefetcher.prefetchFooterAndBuildMetadata();

    // Then: tail prefetching is not triggered
    verifyNoInteractions(parquetPrefetchTailTask);
  }

  @Test
  public void testAddToRecentColumnListProxiesCallsToDependency() {
    // Given: default LogicalIO configuration
    LogicalIOConfiguration logicalIOConfiguration = LogicalIOConfiguration.DEFAULT;
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        mock(ParquetPredictivePrefetchingTask.class);

    ParquetPrefetcher parquetPrefetcher =
        getTestPrefetcher(
            logicalIOConfiguration,
            mock(ParquetColumnPrefetchStore.class),
            mock(ParquetMetadataParsingTask.class),
            mock(ParquetPrefetchTailTask.class),
            mock(ParquetReadTailTask.class),
            mock(ParquetPrefetchRemainingColumnTask.class),
            parquetPredictivePrefetchingTask);

    // When: a column is added to recent list
    parquetPrefetcher.addToRecentColumnList(100);

    // Then: it is also added within the task
    verify(parquetPredictivePrefetchingTask, times(1)).addToRecentColumnList(100);
  }

  private static Stream<Arguments> prefetchRemainingColumnChunkShouldBeSkipped() {
    return Stream.of(
        Arguments.of(true, true), Arguments.of(false, true), Arguments.of(false, false));
  }

  private ParquetReadTailTask getTestParquetReadTailTask() {
    ParquetReadTailTask parquetReadTailTask = mock(ParquetReadTailTask.class);
    when(parquetReadTailTask.readFileTail()).thenReturn(new FileTail(ByteBuffer.allocate(10), 10));
    return parquetReadTailTask;
  }

  private ParquetMetadataParsingTask getTestParquetMetadataTask() {
    ParquetMetadataParsingTask parquetMetadataParsingTask = mock(ParquetMetadataParsingTask.class);
    when(parquetMetadataParsingTask.storeColumnMappers(any()))
        .thenReturn(mock(ColumnMappers.class));
    return parquetMetadataParsingTask;
  }

  private ParquetPrefetcher getTestPrefetcher(
      LogicalIOConfiguration logicalIOConfiguration,
      ParquetColumnPrefetchStore parquetColumnPrefetchStore,
      ParquetMetadataParsingTask parquetMetadataParsingTask,
      ParquetPrefetchTailTask parquetPrefetchTailTask,
      ParquetReadTailTask parquetReadTailTask,
      ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask,
      ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask) {

    return new ParquetPrefetcher(
        TEST_URI,
        logicalIOConfiguration,
        parquetColumnPrefetchStore,
        Telemetry.NOOP,
        parquetMetadataParsingTask,
        parquetPrefetchTailTask,
        parquetReadTailTask,
        parquetPrefetchRemainingColumnTask,
        parquetPredictivePrefetchingTask);
  }
}
