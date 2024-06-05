package com.amazon.connector.s3.io.logical.impl;

import com.amazon.connector.s3.io.logical.LogicalIO;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.object.ObjectMetadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * A basic proxying implementation of a LogicalIO layer. To be extended later with logical
 * optimisations (for example, reading Parquet footers and interpreting Parquet metadata).
 */
public class ParquetLogicalIOImpl implements LogicalIO {

  private final PhysicalIO physicalIO;
  private final LogicalIOConfiguration logicalIOConfiguration;

  private static final Logger LOG = LogManager.getLogger(ParquetLogicalIOImpl.class);
  /**
   * Constructs an instance of LogicalIOImpl.
   *
   * @param physicalIO underlying physical IO that knows how to fetch bytes
   * @param logicalIOConfiguration configuration for this logical IO implementation
   */
  public ParquetLogicalIOImpl(PhysicalIO physicalIO, LogicalIOConfiguration logicalIOConfiguration) {
    this.physicalIO = physicalIO;
    this.logicalIOConfiguration = logicalIOConfiguration;

    CompletableFuture<ObjectMetadata> metadata = this.physicalIO.metadata();
    try {
      if (logicalIOConfiguration.isFooterPrecachingEnabled())
        this.createFooterCachingPlan(metadata);
    } catch (IOException e) {
      LOG.info("There exception during footer prefetching {}", e.toString());
    }
  }

  @Override
  public int read(long position) throws IOException {
    return physicalIO.read(position);
  }

  @Override
  public int read(byte[] buf, int off, int len, long position) throws IOException {
    return physicalIO.read(buf, off, len, position);
  }

  @Override
  public int readTail(byte[] buf, int off, int len) throws IOException {
    return physicalIO.readTail(buf, off, len);
  }

  @Override
  public CompletableFuture<ObjectMetadata> metadata() {
    return physicalIO.metadata();
  }

  @Override
  public void close() throws IOException {
    physicalIO.close();
  }

  private void createFooterCachingPlan(final CompletableFuture<ObjectMetadata> metadata) throws IOException {
    long contentLength = metadata.join().getContentLength();
    long startRange = 0;
    if (contentLength > logicalIOConfiguration.getFooterPrecachingSize())
    {
      boolean smallFileCacheEnabledButFileTooBig = contentLength > logicalIOConfiguration.getSmallObjectSizeThreshold() &&
              logicalIOConfiguration.isSmallObjectsPrefetchingEnabled();

      if (smallFileCacheEnabledButFileTooBig || !logicalIOConfiguration.isSmallObjectsPrefetchingEnabled())
      {
        startRange = contentLength - logicalIOConfiguration.getFooterPrecachingSize();
      }
    }

    List<Range> prefetchRanges = new ArrayList<>();
    prefetchRanges.add(new Range(startRange, contentLength - 1));
    IOPlan ioPlan = IOPlan.builder().prefetchRanges(prefetchRanges).build();
    physicalIO.execute(ioPlan);
  }
}
