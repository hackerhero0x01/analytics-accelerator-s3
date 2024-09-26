package com.amazon.connector.s3.common.telemetry;

import com.amazon.connector.s3.common.Preconditions;
import lombok.*;

/** Represents a measurements for a given {@link TelemetryDatapoint} */
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@EqualsAndHashCode
public abstract class TelemetryDatapointMeasurement {
  /** Metric */
  @NonNull private final TelemetryDatapoint datapoint;

  /** Wall clock time corresponding to metric. */
  private final long epochTimestampNanos;

  /**
   * The actual measurement
   *
   * @return the actual measurement
   */
  public double getValue() {
    return this.getValueCore();
  }

  /**
   * The actual measurement
   *
   * @return the actual measurement
   */
  protected abstract double getValueCore();

  /**
   * Returns the String representation of the {@link TelemetryDatapoint}
   *
   * @return the String representation of the {@link TelemetryDatapoint}.
   */
  @Override
  public String toString() {
    return toString(EpochFormatter.DEFAULT);
  }
  /**
   * Returns the String representation of the {@link TelemetryDatapointMeasurement}
   *
   * @param epochFormatter an instance of {@link EpochFormatter} to format the {@link *
   *     TelemetryDatapointMeasurement#epochTimestampNanos}.
   * @return the String representation of the {@link TelemetryDatapointMeasurement}.
   */
  public abstract String toString(@NonNull EpochFormatter epochFormatter);

  /**
   * The builder base for {@link TelemetryDatapoint}
   *
   * @param <TBuilder> a subtype for {@link TelemetryDatapoint} builder
   */
  public abstract static class TelemetryDatapointMeasurementBuilder<
      T extends TelemetryDatapointMeasurement,
      TBuilder extends TelemetryDatapointMeasurementBuilder<T, TBuilder>> {
    protected static final long UNSET_NANOS = Long.MIN_VALUE;

    @Getter(AccessLevel.PROTECTED)
    private long epochTimestampNanos = UNSET_NANOS;

    /**
     * Sets epoch timestamp.
     *
     * @param epochTimestampNanos epoch timestamp.
     * @return the current instance of {@link OperationMeasurement.OperationMeasurementBuilder}.
     */
    public TBuilder epochTimestampNanos(long epochTimestampNanos) {
      this.epochTimestampNanos = epochTimestampNanos;
      return self();
    }

    /**
     * Returns a strongly typed "self" based on the derived class
     *
     * @return correctly cast 'this'
     */
    @SuppressWarnings("unchecked")
    protected TBuilder self() {
      return (TBuilder) this;
    }

    /**
     * Builds the instance of {@link T}.
     *
     * @return a new instance of {@link T}
     */
    public T build() {
      Preconditions.checkArgument(
          this.epochTimestampNanos >= 0, "The `epochTimestampNanos` must be set and non-negative.");
      return buildCore();
    }

    /**
     * @return new instance of whatever this builder builds
     */
    protected abstract T buildCore();
  }
}
