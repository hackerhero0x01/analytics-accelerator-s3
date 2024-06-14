package com.amazon.connector.s3.io.logical.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManager;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManagerConfiguration;
import com.amazon.connector.s3.io.physical.impl.PhysicalIOImpl;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

public class ParquetLogicalIOImplTest {
  @Test
  void testContructor() {
    assertNotNull(
        new ParquetLogicalIOImpl(
            mock(PhysicalIO.class),
            LogicalIOConfiguration.builder().FooterPrecachingEnabled(false).build()));
  }

  @Test
  void testCloseDependencies() throws IOException {
    // Given
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetLogicalIOImpl logicalIO =
        new ParquetLogicalIOImpl(
            physicalIO, LogicalIOConfiguration.builder().FooterPrecachingEnabled(false).build());

    // When: close called
    logicalIO.close();

    // Then: close will close dependencies
    verify(physicalIO, times(1)).close();
  }

  class IOPlanMatcher implements ArgumentMatcher<IOPlan> {
    private final List<Range> expectedRanges;

    public IOPlanMatcher(List<Range> expectedRanges) {
      this.expectedRanges = expectedRanges;
    }

    @Override
    public boolean matches(IOPlan argument) {
      assertArrayEquals(argument.getPrefetchRanges().toArray(), expectedRanges.toArray());
      return true;
    }
  }

  @Test
  void testFooterCaching() {
    LogicalIOConfiguration configuration =
        LogicalIOConfiguration.builder().FooterPrecachingEnabled(true).build();
    long footerSize = configuration.getFooterPrecachingSize();
    long smallFileSize = configuration.getSmallObjectSizeThreshold();
    HashMap<Long, List<Range>> contentSizeToRanges =
        new HashMap<Long, List<Range>>() {
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
    for (Long contentLength : contentSizeToRanges.keySet()) {
      PhysicalIOImpl mockPhysicalIO = mock(PhysicalIOImpl.class);
      CompletableFuture<ObjectMetadata> metadata =
          CompletableFuture.completedFuture(
              ObjectMetadata.builder().contentLength(contentLength).build());
      when(mockPhysicalIO.metadata()).thenReturn(metadata);
      ParquetLogicalIOImpl logicalIO = new ParquetLogicalIOImpl(mockPhysicalIO, configuration);

      verify(mockPhysicalIO, times(1)).execute(any(IOPlan.class));
      verify(mockPhysicalIO)
          .execute(argThat(new IOPlanMatcher(contentSizeToRanges.get(contentLength))));
    }
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
}
