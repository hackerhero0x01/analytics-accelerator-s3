package com.amazon.connector.s3.io.logical.parquet;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.impl.ParquetMetadataStore;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.util.S3URI;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class ParquetPredictivePrefetchingTaskTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  void testConstructor() {
    assertNotNull(
        new ParquetPredictivePrefetchingTask(
            TEST_URI,
            Telemetry.NOOP,
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
                null,
                Telemetry.NOOP,
                LogicalIOConfiguration.DEFAULT,
                mock(PhysicalIO.class),
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPredictivePrefetchingTask(
                TEST_URI,
                null,
                LogicalIOConfiguration.DEFAULT,
                mock(PhysicalIO.class),
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPredictivePrefetchingTask(
                TEST_URI,
                Telemetry.NOOP,
                null,
                mock(PhysicalIO.class),
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPredictivePrefetchingTask(
                TEST_URI,
                Telemetry.NOOP,
                LogicalIOConfiguration.DEFAULT,
                null,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPredictivePrefetchingTask(
                TEST_URI,
                Telemetry.NOOP,
                LogicalIOConfiguration.DEFAULT,
                mock(PhysicalIO.class),
                null));
  }

  @Test
  void testAddToRecentColumnList() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetMetadataStore parquetMetadataStore = mock(ParquetMetadataStore.class);

    HashMap<Long, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();
    ColumnMetadata columnMetadata =
        new ColumnMetadata(0, "sk_test", 100, 500, "sk_test".hashCode());
    offsetIndexToColumnMap.put(100L, columnMetadata);
    ColumnMappers columnMappers = new ColumnMappers(offsetIndexToColumnMap, new HashMap<>());
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            TEST_URI,
            Telemetry.NOOP,
            LogicalIOConfiguration.DEFAULT,
            physicalIO,
            parquetMetadataStore);

    when(parquetMetadataStore.getColumnMappers(TEST_URI)).thenReturn(columnMappers);

    assertTrue(parquetPredictivePrefetchingTask.addToRecentColumnList(100).isPresent());
    verify(parquetMetadataStore).addRecentColumn(columnMetadata);
  }

  @Test
  void testAddToRecentColumnListEmptyColumnMappers() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetMetadataStore parquetMetadataStore = mock(ParquetMetadataStore.class);

    when(parquetMetadataStore.getColumnMappers(TEST_URI)).thenReturn(null);

    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            TEST_URI,
            Telemetry.NOOP,
            LogicalIOConfiguration.DEFAULT,
            physicalIO,
            parquetMetadataStore);

    assertFalse(parquetPredictivePrefetchingTask.addToRecentColumnList(100).isPresent());
    verify(parquetMetadataStore, times(0)).addRecentColumn(any());
  }

  @Test
  void testPrefetchRecentColumns() throws IOException {
    // Given: prefetching task with some recent columns
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetMetadataStore parquetMetadataStore = mock(ParquetMetadataStore.class);

    StringBuilder columnNames = new StringBuilder();
    columnNames.append("sk_test").append("sk_test_2").append("sk_test_3");
    int schemaHash = columnNames.toString().hashCode();

    HashMap<String, List<ColumnMetadata>> columnNameToColumnMap = new HashMap<>();
    HashMap<Long, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();

    List<ColumnMetadata> sk_testColumnMetadataList = new ArrayList<>();
    ColumnMetadata sk_test1 = new ColumnMetadata(0, "test", 100, 500, schemaHash);
    sk_testColumnMetadataList.add(sk_test1);
    offsetIndexToColumnMap.put(100L, sk_test1);

    List<ColumnMetadata> sk_test_2ColumnMetadataList = new ArrayList<>();
    ColumnMetadata sk_test2 = new ColumnMetadata(0, "sk_test_2", 600, 500, schemaHash);
    sk_test_2ColumnMetadataList.add(sk_test2);
    offsetIndexToColumnMap.put(600L, sk_test2);

    List<ColumnMetadata> sk_test_3ColumnMetadataList = new ArrayList<>();
    ColumnMetadata sk_test3 =
        new ColumnMetadata(0, "sk_test_3", 1100, 500, getHashCode(columnNames));
    sk_test_3ColumnMetadataList.add(sk_test3);
    offsetIndexToColumnMap.put(1100L, sk_test3);

    columnNameToColumnMap.put("sk_test", sk_testColumnMetadataList);
    columnNameToColumnMap.put("sk_test_2", sk_test_2ColumnMetadataList);
    columnNameToColumnMap.put("sk_test_3", sk_test_3ColumnMetadataList);

    Set<String> recentColumns = new HashSet<>();
    recentColumns.add("sk_test");
    recentColumns.add("sk_test_2");
    recentColumns.add("sk_test_3");
    when(parquetMetadataStore.getUniqueRecentColumnsForSchema(schemaHash))
        .thenReturn(recentColumns);

    // When: recent columns get prefetched
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            TEST_URI,
            Telemetry.NOOP,
            LogicalIOConfiguration.DEFAULT,
            physicalIO,
            parquetMetadataStore);
    parquetPredictivePrefetchingTask.prefetchRecentColumns(
        new ColumnMappers(offsetIndexToColumnMap, columnNameToColumnMap));

    // Then: physical IO gets the correct plan
    ArgumentCaptor<IOPlan> ioPlanArgumentCaptor = ArgumentCaptor.forClass(IOPlan.class);
    verify(physicalIO).execute(ioPlanArgumentCaptor.capture());

    IOPlan ioPlan = ioPlanArgumentCaptor.getValue();
    List<Range> expectedRanges = new ArrayList<>();

    expectedRanges.add(new Range(100, 599));
    expectedRanges.add(new Range(600, 1099));
    expectedRanges.add(new Range(1100, 1599));
    assertTrue(ioPlan.getPrefetchRanges().containsAll(expectedRanges));
  }

  @Test
  void testExceptionRemappedToCompletionException() throws IOException {
    // Given: a task performing predictive prefetching
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            TEST_URI,
            Telemetry.NOOP,
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

  private int getHashCode(StringBuilder stringToHash) {
    return stringToHash.toString().hashCode();
  }
}
