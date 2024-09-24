// package com.amazon.connector.s3.common.telemetry;
//
// import java.util.concurrent.ConcurrentHashMap;
// import lombok.*;
//
/// ** Datapoint aggregator */
// @RequiredArgsConstructor
// public class TelemetryDatapointAggregator implements TelemetryReporter {
//  /** Reporter to report the aggregation to */
//  @NonNull @Getter(AccessLevel.PACKAGE)
//  private final TelemetryReporter telemetryReporter;
//  /** Epoch clock. Used to measure the wall time for {@link Operation} start. */
//  @NonNull @Getter(AccessLevel.PACKAGE)
//  private final Clock epochClock;
//  // This is the mapping between the data points and their stats
//  private final ConcurrentHashMap<Metric, Aggregation> dataPoints = new ConcurrentHashMap<>();
//
//  /**
//   * We do nothing here - starting operations are not of interest to us.
//   *
//   * @param epochTimestampNanos wall clock time for the operation start
//   * @param operation and instance of {@link Operation} to start
//   */
//  @Override
//  public void reportStart(long epochTimestampNanos, Operation operation) {}
//
//  /**
//   * This is where
//   *
//   * @param datapointMeasurement an instance of {@link TelemetryDatapointMeasurement}.
//   */
//  @Override
//  public void reportComplete(TelemetryDatapointMeasurement<?> datapointMeasurement) {
//    datapointMeasurement.dataPoint
//  }
//
//  @Override
//  public void flush() {}
//
//  @Getter
//  @AllArgsConstructor
//  private enum AggregationKind {
//    SUM("sum"),
//    COUNT("count"),
//    AVG("avg"),
//    MIN("min"),
//    MAX("max");
//    private final String value;
//  }
//
//  /** A set of aggregations - very primitive so far, just keeping sum and count */
//  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
//  private class Aggregation {
//    @NonNull @Getter TelemetryDatapoint dataPoint;
//    private long count = 0;
//    private double sum = 0;
//    private double min = Double.MIN_VALUE;
//    private double max = Double.MAX_VALUE;
//
//    /**
//     * Records a new value. Note the `synchronized` - necessary to keep the data thread safe This
// is
//     * **very primitive** at the moment and keeps very basic stats
//     *
//     * @param value to record
//     */
//    public synchronized void accumulate(double value) {
//      count++;
//      sum += value;
//      if (value < min) {
//        min = value;
//      }
//      if (value > max) {
//        max = value;
//      }
//    }
//
//    /**
//     * This reports metrics
//     *
//     * @param reporter
//     */
//    public synchronized void report(TelemetryReporter reporter) {
//      long epochTimestampNanos =
// TelemetryDatapointAggregator.this.epochClock.getCurrentTimeNanos();
//      // Always report sum and count
//      reporter.reportComplete(
//          createMetricMeasurement(epochTimestampNanos, AggregationKind.SUM, sum));
//      reporter.reportComplete(
//          createMetricMeasurement(epochTimestampNanos, AggregationKind.COUNT, count));
//
//      // Only report the rest if we have measurements
//      if (count > 0) {
//        reporter.reportComplete(
//            createMetricMeasurement(epochTimestampNanos, AggregationKind.AVG, sum / count));
//        reporter.reportComplete(
//            createMetricMeasurement(epochTimestampNanos, AggregationKind.MAX, max));
//        reporter.reportComplete(
//            createMetricMeasurement(epochTimestampNanos, AggregationKind.MIN, min));
//      }
//    }
//
//    /**
//     * Creates a single measurement
//     *
//     * @param epochTimestampNanos timestamp
//     * @param aggregationKind aggregation kind
//     * @param value value measurement value
//     * @return a new instance of {@link MetricMeasurement}
//     */
//    private MetricMeasurement createMetricMeasurement(
//        long epochTimestampNanos, AggregationKind aggregationKind, double value) {
//      Metric metric =
//          new Metric(dataPoint.getName() + "." + aggregationKind.value,
// dataPoint.getAttributes());
//      return MetricMeasurement.builder()
//          .metric(metric)
//          .kind(MetricMeasurementKind.AGGREGATE)
//          .epochTimestampNanos(epochTimestampNanos)
//          .value(value)
//          .build();
//    }
//  }
// }
