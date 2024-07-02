package com.amazon.connector.s3.io.logical.parquet;

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
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class ParquetPrefetchTailTaskTest {

  @Test
  void testContructor() {
    assertNotNull(
        new ParquetPrefetchTailTask(LogicalIOConfiguration.DEFAULT, mock(PhysicalIO.class)));
  }

  @Test
  void testContructorFailsOnNull() {
    assertThrows(
        NullPointerException.class,
        () -> new ParquetPrefetchTailTask(null, mock(PhysicalIO.class)));
    assertThrows(
        NullPointerException.class,
        () -> new ParquetPrefetchTailTask(LogicalIOConfiguration.DEFAULT, null));
  }

  @Test
  void testTailPrefetch() {

    LogicalIOConfiguration configuration =
        LogicalIOConfiguration.builder().footerCachingEnabled(true).build();

    HashMap<Long, List<Range>> contentSizeToRanges =
        getPrefetchRangeList(
            configuration.getFooterCachingSize(), configuration.getSmallObjectSizeThreshold());

    for (Long contentLength : contentSizeToRanges.keySet()) {
      PhysicalIOImpl mockedPhysicalIO = mock(PhysicalIOImpl.class);
      CompletableFuture<ObjectMetadata> metadata =
          CompletableFuture.completedFuture(
              ObjectMetadata.builder().contentLength(contentLength).build());
      when(mockedPhysicalIO.metadata()).thenReturn(metadata);

      ParquetPrefetchTailTask parquetPrefetchTailTask =
          new ParquetPrefetchTailTask(LogicalIOConfiguration.DEFAULT, mockedPhysicalIO);
      parquetPrefetchTailTask.prefetchTail();

      verify(mockedPhysicalIO).execute(any(IOPlan.class));
      verify(mockedPhysicalIO)
          .execute(argThat(new IOPlanMatcher(contentSizeToRanges.get(contentLength))));
    }
  }

  @Test
  @SneakyThrows
  void testExceptionRemappedToCompletionException() {
    // Given: Parquet Tail Prefetching task
    PhysicalIO mockedPhysicalIO = mock(PhysicalIO.class);
    when(mockedPhysicalIO.getS3URI()).thenReturn(S3URI.of("test", "data"));
    ParquetPrefetchTailTask parquetPrefetchTailTask =
        new ParquetPrefetchTailTask(LogicalIOConfiguration.DEFAULT, mockedPhysicalIO);

    // When: task executes but PhysicalIO throws
    CompletableFuture<ObjectMetadata> metadata =
        CompletableFuture.completedFuture(ObjectMetadata.builder().contentLength(600).build());
    when(mockedPhysicalIO.metadata()).thenReturn(metadata);
    doThrow(new IOException("Error in prefetch")).when(mockedPhysicalIO).execute(any(IOPlan.class));

    // Then: exception is re-mapped to CompletionException
    assertThrows(CompletionException.class, () -> parquetPrefetchTailTask.prefetchTail());
  }

  private HashMap<Long, List<Range>> getPrefetchRangeList(long footerSize, long smallFileSize) {
    return new HashMap<Long, List<Range>>() {
      {
        put(
            1L,
            new ArrayList<Range>() {
              {
                add(new Range(0, 0));
              }
            });
        put(
            footerSize,
            new ArrayList<Range>() {
              {
                add(new Range(0, footerSize - 1));
              }
            });
        put(
            10L + footerSize,
            new ArrayList<Range>() {
              {
                add(new Range(0, footerSize + 9));
              }
            });
        put(
            -1L + smallFileSize,
            new ArrayList<Range>() {
              {
                add(new Range(0, smallFileSize - 2));
              }
            });
        put(
            10L + smallFileSize,
            new ArrayList<Range>() {
              {
                add(new Range(smallFileSize + 10 - footerSize, smallFileSize + 9));
              }
            });
      }
    };
  }
}
