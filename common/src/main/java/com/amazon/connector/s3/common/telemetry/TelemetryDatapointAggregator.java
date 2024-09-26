package com.amazon.connector.s3.common.telemetry;

import com.amazon.connector.s3.common.Preconditions;
import java.util.concurrent.ConcurrentHashMap;
import lombok.*;

/**
 * This class provides a simple metric/operation aggregation functionality. For every metric
 * reported, we extract an aggregation key (currently just the metric name, later attributes as
 * well) and build simple statistics (Min/Max/Avg/Sum/Count). The resulting measurements is then
 * converted into metrics that get sent to the reporter.
 *
 * <p>This class is thread safe.
 */
@RequiredArgsConstructor
public class TelemetryDatapointAggregator implements TelemetryReporter {
  /** Reporter to report the aggregation to */
  @NonNull @Getter(AccessLevel.PACKAGE)
  private final TelemetryReporter telemetryReporter;
  /** Epoch clock. Used to measure the wall time for {@link Operation} start. */
  @NonNull @Getter(AccessLevel.PACKAGE)
  private final Clock epochClock;
  // This is the mapping between the data points and their stats
  private final ConcurrentHashMap<Metric, Aggregation> aggregations = new ConcurrentHashMap<>();

  /**
   * We do nothing here - starting operations are not of interest to us.
   *
   * @param epochTimestampNanos wall clock time for the operation start
   * @param operation and instance of {@link Operation} to start
   */
  @Override
  public void reportStart(long epochTimestampNanos, Operation operation) {}

  /**
   * This is where
   *
   * @param datapointMeasurement an instance of {@link TelemetryDatapointMeasurement}.
   */
  @Override
  public void reportComplete(TelemetryDatapointMeasurement datapointMeasurement) {
    // Right now we do not use attribute-level aggregation and just aggregate by data point names
    // To this end, we create a metric with the same name as the datapoint, but strip the
    // attributes,
    // making this as the key
    Metric aggregationKey =
        Metric.builder().name(datapointMeasurement.getDatapoint().getName()).build();

    Aggregation aggregation =
        aggregations.computeIfAbsent(
            aggregationKey, (key) -> new Aggregation(aggregationKey));
    aggregation.accumulate(datapointMeasurement.getValue());
  }

  @Override
  public void flush() {
    // This is thread-safe, as `values` on ConcurrentHashMap is thread-safe
    // Flush all values for each aggregation into a reporter
    aggregations.values().forEach(aggregation -> aggregation.flush(this.telemetryReporter));
  }

  @Getter
  @AllArgsConstructor
  private enum AggregationKind {
    SUM("sum"),
    COUNT("count"),
    AVG("avg"),
    MIN("min"),
    MAX("max");
    private final String value;
  }

  /** A set of aggregations - very primitive so far, just keeping sum and count */
  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
  private class Aggregation {
    @NonNull @Getter TelemetryDatapoint dataPoint;
    private long count = 0;
    private double sum = 0;
    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;

    /**
     * Records a new value. Note the `synchronized` - necessary to keep the data thread safe This is
     * **very primitive** at the moment and keeps very basic stats
     *
     * @param value to record
     */
    public synchronized void accumulate(double value) {
      count++;
      sum += value;
      if (value < min) {
        min = value;
      }
      if (value > max) {
        max = value;
      }
    }

    /**
     * This reports metrics
     *
     * @param reporter an instance of {@link TelemetryReporter} to report to
     */
    public synchronized void flush(TelemetryReporter reporter) {
      long epochTimestampNanos = TelemetryDatapointAggregator.this.epochClock.getCurrentTimeNanos();
      // We should always have some data points here, because the aggregate wouldn't have been created
      // If we didn't hae any
      Preconditions.checkState(this.count > 0);
      // Always report sum and count
      reporter.reportComplete(
          createMetricMeasurement(epochTimestampNanos, AggregationKind.SUM, sum));
      reporter.reportComplete(
          createMetricMeasurement(epochTimestampNanos, AggregationKind.COUNT, count));
      reporter.reportComplete(
          createMetricMeasurement(epochTimestampNanos, AggregationKind.AVG, sum / count));
      reporter.reportComplete(
          createMetricMeasurement(epochTimestampNanos, AggregationKind.MAX, max));
      reporter.reportComplete(
          createMetricMeasurement(epochTimestampNanos, AggregationKind.MIN, min));
    }

    /**
     * Creates a single measurement
     *
     * @param epochTimestampNanos timestamp
     * @param aggregationKind aggregation kind
     * @param value value measurement value
     * @return a new instance of {@link MetricMeasurement}
     */
    private MetricMeasurement createMetricMeasurement(
        long epochTimestampNanos, AggregationKind aggregationKind, double value) {
      Metric metric =
              Metric.builder().name(dataPoint.getName() + "." + aggregationKind.value).build();
      return MetricMeasurement.builder()
          .metric(metric)
          .kind(MetricMeasurementKind.AGGREGATE)
          .epochTimestampNanos(epochTimestampNanos)
          .value(value)
          .build();
    }
  }
}
