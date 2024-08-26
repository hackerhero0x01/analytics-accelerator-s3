package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.util.S3URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task for prefetching the tail of a parquet file. */
public class ParquetPrefetchTailTask {

  private final S3URI s3URI;
  private final LogicalIOConfiguration logicalIOConfiguration;
  private final PhysicalIO physicalIO;

  private static final Logger LOG = LoggerFactory.getLogger(ParquetPrefetchTailTask.class);

  /**
   * Creates a new instance of {@link ParquetPrefetchTailTask}
   *
   * @param s3URI the S3URI of the object to prefetch
   * @param logicalIOConfiguration LogicalIO configuration
   * @param physicalIO PhysicalIO instance
   */
  public ParquetPrefetchTailTask(
      @NonNull S3URI s3URI,
      @NonNull LogicalIOConfiguration logicalIOConfiguration,
      @NonNull PhysicalIO physicalIO) {
    this.s3URI = s3URI;
    this.logicalIOConfiguration = logicalIOConfiguration;
    this.physicalIO = physicalIO;
  }

  /**
   * Prefetch tail of the parquet file
   *
   * @return range of file prefetched
   */
  public List<Range> prefetchTail() {
    try {
      long contentLength = physicalIO.metadata().getContentLength();
      Optional<Range> tailRangeOptional =
          ParquetUtils.getFileTailRange(logicalIOConfiguration, 0, contentLength);
      // Create a non-empty IOPlan only if we have a valid range to work with
      IOPlan ioPlan = tailRangeOptional.map(IOPlan::new).orElse(IOPlan.EMPTY_PLAN);
      physicalIO.execute(ioPlan);
      return ioPlan.getPrefetchRanges();
    } catch (Exception e) {
      LOG.error(
          "Error in executing tail prefetch plan for {}. Will fallback to reading footer synchronously.",
          this.s3URI.getKey(),
          e);
      throw new CompletionException("Error in executing tail prefetch plan", e);
    }
  }
}
