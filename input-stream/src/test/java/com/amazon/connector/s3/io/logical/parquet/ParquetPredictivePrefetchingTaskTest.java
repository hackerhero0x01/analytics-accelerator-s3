package com.amazon.connector.s3.io.logical.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.impl.ParquetMetadataStore;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class ParquetPredictivePrefetchingTaskTest {

  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  void testConstructor() {
    assertNotNull(
        new ParquetPredictivePrefetchingTask(
            TEST_URI,
            LogicalIOConfiguration.DEFAULT,
            mock(PhysicalIO.class),
            new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT)));
  }

  @Test
  void testConstructorFailsOnNull() {
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPredictivePrefetchingTask(
                TEST_URI,
                LogicalIOConfiguration.DEFAULT,
                null,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPredictivePrefetchingTask(
                TEST_URI,
                null,
                mock(PhysicalIO.class),
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT)));
  }

  @Test
  void testAddToRecentColumnList() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetMetadataStore parquetMetadataStore = mock(ParquetMetadataStore.class);

    HashMap<Long, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();
    offsetIndexToColumnMap.put(100L, new ColumnMetadata(0, "sk_test", 100, 500));
    ColumnMappers columnMappers = new ColumnMappers(offsetIndexToColumnMap, new HashMap<>());
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            TEST_URI, LogicalIOConfiguration.DEFAULT, physicalIO, parquetMetadataStore);

    when(parquetMetadataStore.getColumnMappers(TEST_URI)).thenReturn(columnMappers);

    assertTrue(parquetPredictivePrefetchingTask.addToRecentColumnList(100).isPresent());
    verify(parquetMetadataStore).addRecentColumn("sk_test");
  }

  @Test
  void testAddToRecentColumnListEmptyColumnMappers() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetMetadataStore parquetMetadataStore = mock(ParquetMetadataStore.class);

    when(parquetMetadataStore.getColumnMappers(TEST_URI)).thenReturn(null);

    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            TEST_URI, LogicalIOConfiguration.DEFAULT, physicalIO, parquetMetadataStore);

    assertFalse(parquetPredictivePrefetchingTask.addToRecentColumnList(100).isPresent());
    verify(parquetMetadataStore, times(0)).addRecentColumn(anyString());
  }

  @Test
  void testPrefetchRecentColumns() throws IOException {
    // Given: prefetching task with some recent columns
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetMetadataStore parquetMetadataStore = mock(ParquetMetadataStore.class);

    HashMap<String, List<ColumnMetadata>> columnNameToColumnMap = new HashMap<>();
    List<ColumnMetadata> columnMetadataList = new ArrayList<>();
    columnMetadataList.add(new ColumnMetadata(0, "sk_test", 100, 500));
    columnNameToColumnMap.put("sk_test", columnMetadataList);

    Set<String> recentColumns = new HashSet<>();
    recentColumns.add("sk_test");
    when(parquetMetadataStore.getRecentColumns()).thenReturn(recentColumns);

    // When: recent columns get prefetched
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            TEST_URI, LogicalIOConfiguration.DEFAULT, physicalIO, parquetMetadataStore);
    parquetPredictivePrefetchingTask.prefetchRecentColumns(
        new ColumnMappers(new HashMap<>(), columnNameToColumnMap));

    // Then: physical IO gets the correct plan
    ArgumentCaptor<IOPlan> ioPlanArgumentCaptor = ArgumentCaptor.forClass(IOPlan.class);
    verify(physicalIO).execute(ioPlanArgumentCaptor.capture());

    IOPlan ioPlan = ioPlanArgumentCaptor.getValue();
    List<Range> expectedRanges = new ArrayList<>();
    expectedRanges.add(new Range(100, 600));
    assertEquals(ioPlan.getPrefetchRanges(), expectedRanges);
  }

  @Test
  void testExceptionRemappedToCompletionException() throws IOException {
    // Given: a task performing predictive prefetching
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            TEST_URI,
            LogicalIOConfiguration.DEFAULT,
            physicalIO,
            new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT));

    // When: the underlying PhysicalIO always throws
    doThrow(new IOException("Error in prefetch")).when(physicalIO).execute(any(IOPlan.class));

    // Then: exceptions are wrapped by CompletionExceptions
    assertThrows(
        CompletionException.class,
        () ->
            parquetPredictivePrefetchingTask.prefetchRecentColumns(
                new ColumnMappers(new HashMap<>(), new HashMap<>())));
  }

  @Test
  void testParquetMetadataStoreCacheEviction() {
    ParquetMetadataStore parquetMetadataStore =
        new ParquetMetadataStore(
            LogicalIOConfiguration.builder().parquetMetadataStoreSize(2).build());

    parquetMetadataStore.addRecentColumn("column1");
    parquetMetadataStore.addRecentColumn("column2");
    parquetMetadataStore.addRecentColumn("column3");

    Assertions.assertTrue(parquetMetadataStore.getRecentColumns().size() == 2);
    Assertions.assertFalse(parquetMetadataStore.getRecentColumns().contains("column1"));
    Assertions.assertTrue(parquetMetadataStore.getRecentColumns().contains("column2"));
  }
}
