package com.amazon.connector.s3.io.logical.parquet;

import static com.amazon.connector.s3.util.Constants.ONE_MB;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.impl.PhysicalIOImpl;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.Range;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ParquetPrefetchRemainingColumnTaskTest {

  @Test
  void testContructor() {
    assertNotNull(
        new ParquetPrefetchRemainingColumnTask(
            LogicalIOConfiguration.DEFAULT, mock(PhysicalIO.class)));
  }

  @Test
  void testContructorFailsOnNull() {
    assertThrows(
        NullPointerException.class,
        () -> new ParquetPrefetchRemainingColumnTask(LogicalIOConfiguration.DEFAULT, null));
    assertThrows(
        NullPointerException.class,
        () -> new ParquetPrefetchRemainingColumnTask(null, mock(PhysicalIO.class)));
  }

  @Test
  void testRemainingColumnPrefetched() {

    HashMap<String, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();
    offsetIndexToColumnMap.put("200", new ColumnMetadata(0, "ss_sold_date_sk", 200, 10 * ONE_MB));

    PhysicalIOImpl mockedPhysicalIO = mock(PhysicalIOImpl.class);
    when(mockedPhysicalIO.columnMappers())
        .thenReturn(new ColumnMappers(offsetIndexToColumnMap, new HashMap<>()));

    List<Range> expectedRanges = new ArrayList<>();
    // If a column starts at 200, has size 10MB, and we get a read for 5MB, then queue a
    // prefetch with range (200 + 5MB) to (200 + 5MB + (10MB - 5MB)).
    // Which means prefetch the remainder of the column chunk.
    int FIVE_MB = 5 * ONE_MB;
    int TEN_MB = 10 * ONE_MB;
    expectedRanges.add(new Range(200 + FIVE_MB, 200 + FIVE_MB + (TEN_MB - FIVE_MB)));

    ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask =
        new ParquetPrefetchRemainingColumnTask(LogicalIOConfiguration.DEFAULT, mockedPhysicalIO);
    parquetPrefetchRemainingColumnTask.prefetchRemainingColumnChunk(200, 5 * ONE_MB);

    verify(mockedPhysicalIO).execute(any(IOPlan.class));
    verify(mockedPhysicalIO).execute(argThat(new IOPlanMatcher(expectedRanges)));
  }

  @Test
  void testExceptionSwallowed() {
    HashMap<String, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();
    offsetIndexToColumnMap.put("200", new ColumnMetadata(0, "ss_sold_date_sk", 200, 10 * ONE_MB));

    PhysicalIOImpl mockedPhysicalIO = mock(PhysicalIOImpl.class);
    when(mockedPhysicalIO.columnMappers())
        .thenReturn(new ColumnMappers(offsetIndexToColumnMap, new HashMap<>()));
    ParquetPrefetchRemainingColumnTask parquetPrefetchRemainingColumnTask =
        new ParquetPrefetchRemainingColumnTask(LogicalIOConfiguration.DEFAULT, mockedPhysicalIO);

    doThrow(new IOException("Error in prefetch")).when(mockedPhysicalIO).execute(any(IOPlan.class));

    assertFalse(
        parquetPrefetchRemainingColumnTask
            .prefetchRemainingColumnChunk(200, 5 * ONE_MB)
            .isPresent());
  }
}
