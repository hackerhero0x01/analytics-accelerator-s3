package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.common.telemetry.Operation;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.impl.ParquetMetadataStore;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.IOPlanExecution;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.util.S3URI;
import com.amazon.connector.s3.util.StreamAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
  private final Telemetry telemetry;
  private final PhysicalIO physicalIO;
  private final ParquetMetadataStore parquetMetadataStore;
  private final LogicalIOConfiguration logicalIOConfiguration;
  private static final String OPERATION_PARQUET_PREFETCH_COLUMNS = "parquet.task.prefetch.columns";
  private static final Logger LOG = LoggerFactory.getLogger(ParquetPredictivePrefetchingTask.class);

  /**
   * Creates a new instance of {@link ParquetPredictivePrefetchingTask}
   *
   * @param s3Uri the object's S3URI
   * @param telemetry an instance of {@link Telemetry} to use
   * @param logicalIOConfiguration logical io configuration
   * @param physicalIO PhysicalIO instance
   * @param parquetMetadataStore object containing Parquet usage information
   */
  public ParquetPredictivePrefetchingTask(
      @NonNull S3URI s3Uri,
      @NonNull Telemetry telemetry,
      @NonNull LogicalIOConfiguration logicalIOConfiguration,
      @NonNull PhysicalIO physicalIO,
      @NonNull ParquetMetadataStore parquetMetadataStore) {
    this.s3Uri = s3Uri;
    this.telemetry = telemetry;
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
        ColumnMetadata columnMetadata = columnMappers.getOffsetIndexToColumnMap().get(position);
        parquetMetadataStore.addRecentColumn(columnMetadata);
        return Optional.of(columnMetadata.getColumnName());
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
    return telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_PARQUET_PREFETCH_COLUMNS)
                .attribute(StreamAttributes.uri(this.s3Uri))
                .build(),
        () -> {
          List<Range> prefetchRanges = new ArrayList<>();
          for (String recentColumn : getRecentColumns(columnMappers.getOffsetIndexToColumnMap())) {
            if (columnMappers.getColumnNameToColumnMap().containsKey(recentColumn)) {
              LOG.debug(
                  "Column {} found in schema for {}, adding to prefetch list",
                  recentColumn,
                  this.s3Uri.getKey());
              List<ColumnMetadata> columnMetadataList =
                  columnMappers.getColumnNameToColumnMap().get(recentColumn);
              for (ColumnMetadata columnMetadata : columnMetadataList) {
                prefetchRanges.add(
                    new Range(
                        columnMetadata.getStartPos(),
                        columnMetadata.getStartPos() + columnMetadata.getCompressedSize() - 1));
              }
            }
          }

          IOPlan ioPlan =
              (prefetchRanges.isEmpty()) ? IOPlan.EMPTY_PLAN : new IOPlan(prefetchRanges);
          try {
            return physicalIO.execute(ioPlan);
          } catch (Exception e) {
            LOG.error(
                "Error in executing predictive prefetch plan for {}. Will fallback to synchronous reading for this key.",
                this.s3Uri.getKey(),
                e);
            throw new CompletionException("Error in executing predictive prefetching", e);
          }
        });
  }

  private Set<String> getRecentColumns(HashMap<Long, ColumnMetadata> offsetIndexToColumnMap) {
    Set<String> recentColumns = new HashSet<>();

    if (offsetIndexToColumnMap.size() > 0) {
      Map.Entry<Long, ColumnMetadata> firstColumnData =
          offsetIndexToColumnMap.entrySet().iterator().next();

      int schemaHash = firstColumnData.getValue().getSchemaHash();
      recentColumns = parquetMetadataStore.getUniqueRecentColumnsForSchema(schemaHash);
    }

    return recentColumns;
  }
}
