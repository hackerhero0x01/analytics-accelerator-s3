package com.amazon.connector.s3.io.logical.parquet;

import static com.amazon.connector.s3.util.Constants.ONE_MB;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.impl.ParquetMetadataStore;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.impl.PhysicalIOImpl;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.util.S3URI;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class ParquetPrefetchRemainingColumnTaskTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  void testConstructor() {
    assertNotNull(
        new ParquetPrefetchRemainingColumnTask(
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
            new ParquetPrefetchRemainingColumnTask(
                TEST_URI,
                LogicalIOConfiguration.DEFAULT,
                null,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPrefetchRemainingColumnTask(
                TEST_URI,
                null,
                mock(PhysicalIO.class),
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT)));
  }

  @Test
  void testRemainingColumnPrefetched() {
    HashMap<Long, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();
    offsetIndexToColumnMap.put(
        200L,
        new ColumnMetadata(0, "ss_sold_date_sk", 200, 10 * ONE_MB, "ss_sold_date_sk".hashCode()));

    ParquetMetadataStore mockedParquetMetadataStore = mock(ParquetMetadataStore.class);
    PhysicalIOImpl mockedPhysicalIO = mock(PhysicalIOImpl.class);
    when(mockedParquetMetadataStore.getColumnMappers(TEST_URI))
        .thenReturn(new ColumnMappers(offsetIndexToColumnMap, new HashMap<>()));

    List<Range> expectedRanges = new ArrayList<>();
    // If a column starts at 200, has size 10MB, and we get a read for 5MB, then queue a
    // prefetch with range (200 + 5MB) to (200 + 5MB + (10MB - 5MB)).
    // Which means prefetch the remainder of the column chunk.
    int FIVE_MB = 5 * ONE_MB;
    int TEN_MB = 10 * ONE_MB;
    expectedRanges.add(new Range(200 + FIVE_MB, 200 + FIVE_MB + (TEN_MB - FIVE_MB)));

    ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask =
        new ParquetPrefetchRemainingColumnTask(
            TEST_URI, LogicalIOConfiguration.DEFAULT, mockedPhysicalIO, mockedParquetMetadataStore);
    parquetPrefetchRemainingColumnTask.prefetchRemainingColumnChunk(200, 5 * ONE_MB);

    verify(mockedPhysicalIO).execute(any(IOPlan.class));
    verify(mockedPhysicalIO).execute(argThat(new IOPlanMatcher(expectedRanges)));
  }

  @Test
  void testExceptionRemappedToCompletionException() {
    HashMap<Long, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();
    offsetIndexToColumnMap.put(
        200L,
        new ColumnMetadata(0, "ss_sold_date_sk", 200, 10 * ONE_MB, "ss_sold_date_sk".hashCode()));

    ParquetMetadataStore mockedParquetMetadataStore = mock(ParquetMetadataStore.class);
    PhysicalIOImpl mockedPhysicalIO = mock(PhysicalIOImpl.class);

    when(mockedParquetMetadataStore.getColumnMappers(TEST_URI))
        .thenReturn(new ColumnMappers(offsetIndexToColumnMap, new HashMap<>()));
    ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask =
        new ParquetPrefetchRemainingColumnTask(
            TEST_URI, LogicalIOConfiguration.DEFAULT, mockedPhysicalIO, mockedParquetMetadataStore);

    doThrow(new IOException("Error in prefetch")).when(mockedPhysicalIO).execute(any(IOPlan.class));

    assertThrows(
        CompletionException.class,
        () -> parquetPrefetchRemainingColumnTask.prefetchRemainingColumnChunk(200, 5 * ONE_MB));
  }
}
