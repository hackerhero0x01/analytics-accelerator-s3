package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task for prefetching the tail of a parquet file. */
public class ParquetPrefetchTailTask {

  private final LogicalIOConfiguration logicalIOConfiguration;
  private final PhysicalIO physicalIO;

  private static final Logger LOG = LoggerFactory.getLogger(ParquetPrefetchTailTask.class);

  /**
   * Creates a new instance of {@link ParquetPrefetchTailTask}
   *
   * @param logicalIOConfiguration logical io configuration
   * @param physicalIO physicalIO instance
   */
  public ParquetPrefetchTailTask(
      @NonNull LogicalIOConfiguration logicalIOConfiguration, @NonNull PhysicalIO physicalIO) {
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
      long contentLength = physicalIO.metadata().join().getContentLength();
      Range tailRange = ParquetUtils.getFileTailRange(logicalIOConfiguration, 0, contentLength);

      List<Range> prefetchRanges = new ArrayList<>();
      prefetchRanges.add(tailRange);
      IOPlan ioPlan = IOPlan.builder().prefetchRanges(prefetchRanges).build();
      physicalIO.execute(ioPlan);
      return prefetchRanges;
    } catch (Exception e) {
      LOG.error("Error in executing tail prefetch plan for {}. Will fallback to reading footer synchronously.", physicalIO.getS3URI().getKey(), e);
      throw new CompletionException("Error in executing tail prefetch plan", e);
    }
  }
}
