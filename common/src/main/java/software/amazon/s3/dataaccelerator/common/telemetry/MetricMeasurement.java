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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.s3.dataaccelerator.common.Preconditions;

/** Represents telemetry for the metric measurement. */
@EqualsAndHashCode(callSuper = true)
public class MetricMeasurement extends TelemetryDatapointMeasurement {
  /** Kind of metric measurement */
  @NonNull @Getter private final MetricMeasurementKind kind;
  /** Metric value. */
  private final double value;

  /**
   * Creates a new instance of {@link MetricMeasurement}
   *
   * @param metric instance of {@link Metric}
   * @param kind metric kind
   * @param epochTimestampNanos timestamp
   * @param value metric value
   */
  private MetricMeasurement(
      Metric metric, @NonNull MetricMeasurementKind kind, long epochTimestampNanos, double value) {
    super(metric, epochTimestampNanos);
    this.kind = kind;
    this.value = value;
  }

  /**
   * Returns the underlying {@link TelemetryDatapoint}.
   *
   * @return the underlying {@link TelemetryDatapoint}
   */
  @Override
  protected double getValueCore() {
    return value;
  }

  /**
   * Underlying {@link Metric}
   *
   * @return underlying {@link Metric}
   */
  public Metric getMetric() {
    return (Metric) this.getDatapoint();
  }

  @Override
  public String toString(
      @NonNull TelemetryFormat telemetryFormat, @NonNull EpochFormatter epochFormatter) {
    return telemetryFormat.renderMetricMeasurement(this, epochFormatter);
  }

  /**
   * Creates a new {@link MetricMeasurementBuilder}.
   *
   * @return a new instance of {@link MetricMeasurementBuilder}.
   */
  public static MetricMeasurementBuilder builder() {
    return new MetricMeasurementBuilder();
  }

  /** Builder for {@link MetricMeasurement} */
  public static class MetricMeasurementBuilder
      extends TelemetryDatapointMeasurementBuilder<MetricMeasurement, MetricMeasurementBuilder> {
    private Metric metric;
    private MetricMeasurementKind kind = MetricMeasurementKind.RAW;
    double value = Double.NaN;

    /**
     * Sets metric.
     *
     * @param metric metric.
     * @return the current instance of {@link MetricMeasurementBuilder}.
     */
    public MetricMeasurementBuilder metric(@NonNull Metric metric) {
      this.metric = metric;
      return this;
    }

    /**
     * Sets metric kind
     *
     * @param kind kind.
     * @return the current instance of {@link MetricMeasurementBuilder}.
     */
    public MetricMeasurementBuilder kind(@NonNull MetricMeasurementKind kind) {
      this.kind = kind;
      return this;
    }

    /**
     * Sets metric value
     *
     * @param value metric value
     * @return the current instance of {@link MetricMeasurementBuilder}.
     */
    public MetricMeasurementBuilder value(double value) {
      this.value = value;
      return this;
    }

    /**
     * Builds the new {@link MetricMeasurement}.
     *
     * @return a new instance of {@link MetricMeasurement}.
     */
    @Override
    protected MetricMeasurement buildCore() {
      Preconditions.checkNotNull(metric, "The `metric` must be set.");
      Preconditions.checkArgument(!Double.isNaN(this.value), "The `value` must be set.");

      return new MetricMeasurement(
          this.metric, this.kind, this.getEpochTimestampNanos(), this.value);
    }
  }
}
