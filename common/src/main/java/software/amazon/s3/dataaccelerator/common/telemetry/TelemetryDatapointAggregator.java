/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.dataaccelerator.common.telemetry;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.dataaccelerator.common.Preconditions;

/**
 * This class provides a simple metric/operation aggregation functionality. For every metric
 * reported, we extract an aggregation key (currently just the metric name, later attributes as
 * well) and build simple statistics (Min/Max/Avg/Sum/Count). The resulting measurements is then
 * converted into metrics that get sent to the reporter.
 *
 * <p>This class is thread safe.
 */
public class TelemetryDatapointAggregator implements TelemetryReporter {
  /** Reporter to report the aggregation to */
  @NonNull @Getter private final TelemetryReporter telemetryReporter;
  /** Epoch clock. Used to measure the wall time for {@link Operation} start. */
  @NonNull @Getter private final Clock epochClock;
  /** This is the mapping between the data points and their stats * */
  @NonNull @Getter(AccessLevel.PACKAGE)
  private final ConcurrentHashMap<Metric, Aggregation> aggregations = new ConcurrentHashMap<>();
  /** This is the task that flushes data on a regular basis, if set up */
  private final AtomicReference<ScheduledExecutorService> flushTask;

  private static final Logger LOG = LoggerFactory.getLogger(TelemetryDatapointAggregator.class);

  /**
   * Creates a new instance of {@link TelemetryDatapointAggregator}
   *
   * @param telemetryReporter an instance of {@link TelemetryReporter} to report data to
   * @param flushInterval interval to flush aggregates at. If set to None, only explicit flushes wil
   *     flush aggregates
   */
  public TelemetryDatapointAggregator(
      TelemetryReporter telemetryReporter, Optional<Duration> flushInterval) {
    this(telemetryReporter, flushInterval, DefaultEpochClock.DEFAULT);
  }

  /**
   * Creates a new instance of {@link TelemetryDatapointAggregator}
   *
   * @param telemetryReporter an instance of {@link TelemetryReporter} to report data to
   * @param epochClock wall clock
   * @param flushInterval interval to flush aggregates at. If set to None, only explicit flushes wil
   *     flush aggregates
   */
  @SuppressFBWarnings(
      value = "MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR",
      justification =
          "We don't actually call a virtual method here, rather set it up for scheduling.")
  TelemetryDatapointAggregator(
      @NonNull TelemetryReporter telemetryReporter,
      @NonNull Optional<Duration> flushInterval,
      @NonNull Clock epochClock) {
    this.telemetryReporter = telemetryReporter;
    this.epochClock = epochClock;
    if (flushInterval.isPresent()) {
      ScheduledExecutorService scheduledExecutorService =
          Executors.newSingleThreadScheduledExecutor();
      scheduledExecutorService.scheduleAtFixedRate(
          this::flush,
          flushInterval.get().toNanos(),
          flushInterval.get().toNanos(),
          TimeUnit.NANOSECONDS);
      this.flushTask = new AtomicReference<>(scheduledExecutorService);
    } else {
      this.flushTask = new AtomicReference<>();
    }
  }

  /** Closes resources associated with the aggregator. */
  @Override
  public void close() {
    try {
      // To allow multiple close calls and avoid racing on concurrency
      // we first swap out the value for null, and if the current value of scheduledExecutorService
      // is not null, we shut it down.
      // This guarantees that, if close is called, we shut down the task exactly once
      ScheduledExecutorService scheduledExecutorService = this.flushTask.getAndSet(null);
      if (scheduledExecutorService != null) {
        scheduledExecutorService.shutdownNow();
      }
    } catch (Exception e) {
      // do not allow exceptions from shut leak out, as, regardless of the outcome, we will no
      // longer flush, which is the point
      LOG.debug("Error shutting down flush task in TelemetryDatapointAggregator", e);
    }
  }

  /**
   * We do nothing here - starting operations are not of interest to us.
   *
   * @param epochTimestampNanos wall clock time for the operation start
   * @param operation and instance of {@link Operation} to start
   */
  @Override
  public void reportStart(long epochTimestampNanos, Operation operation) {}

  /**
   * Updates the aggregations with {@link TelemetryDatapointMeasurement}
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
        aggregations.computeIfAbsent(aggregationKey, (key) -> new Aggregation(aggregationKey));
    aggregation.accumulate(datapointMeasurement.getValue());
  }

  @Override
  public void flush() {
    LOG.debug("Flushing aggregates");
    // This is thread-safe, as `values` on ConcurrentHashMap is thread-safe
    // Flush all values for each aggregation into a reporter
    aggregations.values().forEach(aggregation -> aggregation.flush(this.telemetryReporter));
  }

  @Getter
  @AllArgsConstructor
  enum AggregationKind {
    SUM("sum"),
    COUNT("count"),
    AVG("avg"),
    MIN("min"),
    MAX("max");
    private final String value;
  }

  /** A set of aggregations - very primitive so far, just keeping sum and count */
  @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
  class Aggregation {
    @Getter(onMethod_ = {@Synchronized})
    @NonNull private final TelemetryDatapoint datapoint;

    @Getter(onMethod_ = {@Synchronized})
    private long count = 0;

    @Getter(onMethod_ = {@Synchronized})
    private double sum = 0;

    @Getter(onMethod_ = {@Synchronized})
    private double min = Double.MAX_VALUE;

    @Getter(onMethod_ = {@Synchronized})
    private double max = Double.MIN_VALUE;

    /**
     * Records a new value. Note the `synchronized` - necessary to keep the data thread safe This is
     * **very primitive** at the moment and keeps very basic stats
     *
     * @param value to record
     */
    @Synchronized
    public void accumulate(double value) {
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
    @Synchronized
    public void flush(TelemetryReporter reporter) {
      long epochTimestampNanos = TelemetryDatapointAggregator.this.epochClock.getCurrentTimeNanos();
      // We should always have some data points here, because the aggregate wouldn't have been
      // created
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
          Metric.builder().name(datapoint.getName() + "." + aggregationKind.value).build();
      return MetricMeasurement.builder()
          .metric(metric)
          .kind(MetricMeasurementKind.AGGREGATE)
          .epochTimestampNanos(epochTimestampNanos)
          .value(value)
          .build();
    }
  }
}
