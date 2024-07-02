package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.impl.ParquetMetadataStore;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.IOPlanExecution;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.util.S3URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task for predictively prefetching columns of a parquet file. Works by keeping track of recent
 * columns read from previously opened parquet files, and if the currently open parquet file has a
 * recent column, prefetch it.
 */
public class ParquetPredictivePrefetchingTask {

  private final S3URI s3Uri;
  private final PhysicalIO physicalIO;
  private final ParquetMetadataStore parquetMetadataStore;
  private final LogicalIOConfiguration logicalIOConfiguration;

  private static final Logger LOG = LoggerFactory.getLogger(ParquetPredictivePrefetchingTask.class);

  /**
   * Creates a new instance of {@link ParquetPredictivePrefetchingTask}
   *
   * @param s3Uri the object's S3URI
   * @param logicalIOConfiguration logical io configuration
   * @param physicalIO PhysicalIO instance
   * @param parquetMetadataStore object containing Parquet usage information
   */
  public ParquetPredictivePrefetchingTask(
      @NonNull S3URI s3Uri,
      @NonNull LogicalIOConfiguration logicalIOConfiguration,
      @NonNull PhysicalIO physicalIO,
      @NonNull ParquetMetadataStore parquetMetadataStore) {
    this.s3Uri = s3Uri;
    this.physicalIO = physicalIO;
    this.logicalIOConfiguration = logicalIOConfiguration;
    this.parquetMetadataStore = parquetMetadataStore;
  }

  /**
   * Checks if the current position corresponds to a column, and if yes, adds it to the recent
   * columns list.
   *
   * @param position current read position
   * @return name of column added as recent column
   */
  public Optional<String> addToRecentColumnList(long position) {
    if (logicalIOConfiguration.isPredictivePrefetchingEnabled()
        && parquetMetadataStore.getColumnMappers(s3Uri) != null) {
      ColumnMappers columnMappers = parquetMetadataStore.getColumnMappers(s3Uri);
      if (columnMappers.getOffsetIndexToColumnMap().containsKey(position)) {
        String recentColumnName =
            columnMappers.getOffsetIndexToColumnMap().get(position).getColumnName();
        parquetMetadataStore.addRecentColumn(recentColumnName);
        return Optional.of(recentColumnName);
      }
    }

    return Optional.empty();
  }

  /**
   * If any recent columns exist in the current parquet file, prefetch them.
   *
   * @param columnMappers Parquet file column mappings
   * @return ranges prefetched
   */
  public IOPlanExecution prefetchRecentColumns(ColumnMappers columnMappers) {
    List<Range> prefetchRanges = new ArrayList<>();
    for (String recentColumn : parquetMetadataStore.getRecentColumns()) {
      if (columnMappers.getColumnNameToColumnMap().containsKey(recentColumn)) {
        LOG.debug(
                "Column {} found in schema for {}, adding to prefetch list",
                recentColumn,
                physicalIO.getS3URI().getKey());
        List<ColumnMetadata> columnMetadataList =
            columnMappers.getColumnNameToColumnMap().get(recentColumn);
        for (ColumnMetadata columnMetadata : columnMetadataList) {
          prefetchRanges.add(
              new Range(
                  columnMetadata.getStartPos(),
                  columnMetadata.getStartPos() + columnMetadata.getCompressedSize()));
        }
      }
    }

    IOPlan ioPlan = IOPlan.builder().prefetchRanges(prefetchRanges).build();
    try {
      return physicalIO.execute(ioPlan);
    } catch (Exception e) {
      LOG.error("Error in executing predictive prefetch plan for {}. Will fallback to synchronous reading for this key.", physicalIO.getS3URI().getKey(), e);
      throw new CompletionException("Error in executing predictive prefetching", e);
    }
  }
}
