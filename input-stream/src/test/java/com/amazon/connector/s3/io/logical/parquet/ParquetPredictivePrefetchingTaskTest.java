package com.amazon.connector.s3.io.logical.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.Range;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class ParquetPredictivePrefetchingTaskTest {

  @Test
  void testContructor() {
    assertNotNull(
        new ParquetPredictivePrefetchingTask(
            LogicalIOConfiguration.DEFAULT, mock(PhysicalIO.class)));
  }

  @Test
  void testContructorFailsOnNull() {
    assertThrows(
        NullPointerException.class,
        () -> new ParquetPredictivePrefetchingTask(LogicalIOConfiguration.DEFAULT, null));
    assertThrows(
        NullPointerException.class,
        () -> new ParquetPredictivePrefetchingTask(null, mock(PhysicalIO.class)));
  }

  @Test
  void testAddToRecentColumnList() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);

    HashMap<String, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();
    offsetIndexToColumnMap.put("100", new ColumnMetadata(0, "sk_test", 100, 500));
    ColumnMappers columnMappers = new ColumnMappers(offsetIndexToColumnMap, new HashMap<>());
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(LogicalIOConfiguration.DEFAULT, physicalIO);

    when(physicalIO.columnMappers()).thenReturn(columnMappers);

    assertTrue(parquetPredictivePrefetchingTask.addToRecentColumnList(100).isPresent());
    verify(physicalIO).addRecentColumn("sk_test");
  }

  @Test
  void testAddToRecentColumnListEmptyColumnMappers() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    when(physicalIO.columnMappers()).thenReturn(null);

    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(LogicalIOConfiguration.DEFAULT, physicalIO);

    assertFalse(parquetPredictivePrefetchingTask.addToRecentColumnList(100).isPresent());
    assertFalse(
        parquetPredictivePrefetchingTask.prefetchRecentColumns(Optional.empty()).isPresent());
    verify(physicalIO, times(0)).addRecentColumn(anyString());
  }

  @Test
  void testPredictivePrefetchingDisabled() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    when(physicalIO.columnMappers()).thenReturn(null);

    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            LogicalIOConfiguration.builder().predictivePrefetchingEnabled(false).build(),
            physicalIO);
    parquetPredictivePrefetchingTask.addToRecentColumnList(100);

    assertFalse(parquetPredictivePrefetchingTask.addToRecentColumnList(100).isPresent());
    assertFalse(
        parquetPredictivePrefetchingTask
            .prefetchRecentColumns(Optional.of(new ColumnMappers(new HashMap<>(), new HashMap<>())))
            .isPresent());
    verify(physicalIO, times(0)).addRecentColumn(anyString());
  }

  @Test
  void testPrefetchRecentColumns() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    HashMap<String, ColumnMetadata> columnNameToColumnMap = new HashMap<>();
    columnNameToColumnMap.put("sk_test", new ColumnMetadata(0, "sk_test", 100, 500));

    Set<String> recentColums = new HashSet<>();
    recentColums.add("sk_test");
    when(physicalIO.getRecentColumns()).thenReturn(recentColums);

    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(LogicalIOConfiguration.DEFAULT, physicalIO);
    Optional<List<Range>> prefetchedRanges =
        parquetPredictivePrefetchingTask.prefetchRecentColumns(
            Optional.of(new ColumnMappers(new HashMap<>(), columnNameToColumnMap)));

    assertTrue(prefetchedRanges.isPresent());

    List<Range> expectedRanges = new ArrayList<>();
    expectedRanges.add(new Range(100, 600));

    assertEquals(prefetchedRanges.get(), expectedRanges);

    verify(physicalIO).execute(any(IOPlan.class));
  }
}
