package com.amazon.connector.s3.common.telemetry;

import com.amazon.connector.s3.common.Preconditions;
import java.util.Optional;
import lombok.NonNull;
import lombok.Value;

/** Represents telemetry for the operation measurement. */
@Value
public class OperationMeasurement {
  /** Operation */
  @NonNull Operation operation;
  /** Wall clock time corresponding to operation start. */
  long epochTimestampNanos;
  /** Elapsed clock time corresponding to operation start. */
  long elapsedStartTimeNanos;
  /** Elapsed clock time corresponding to operation completion. */
  long elapsedCompleteTimeNanos;
  /** Exception thrown as part of the execution. */
  @NonNull Optional<Throwable> error;

  public static final String DEFAULT_FORMAT_STRING = "[%s] [%s] %s: %,d ns";
  private static final String DEFAULT_ERROR_FORMAT_STRING = " [%s: '%s']";
  private static final String EXCEPTION_FORMAT = "";
  private static final String SUCCESS = "success";
  private static final String FAILURE = "failure";

  /**
   * Returns the String representation of the {@link OperationMeasurement}. {@link
   * OperationMeasurement#DEFAULT_FORMAT_STRING} will be used to format the string. The parameters
   * are supplied in the following order: 1 - start epoch, String 2 - success of failure, String 3 -
   * operation, String 4 - elapsed time in nanos, Long.
   *
   * @return the String representation of the {@link OperationMeasurement}.
   */
  @Override
  public String toString() {
    return toString(EpochFormatter.DEFAULT);
  }

  /**
   * Returns the String representation of the {@link OperationMeasurement}. {@link
   * OperationMeasurement#DEFAULT_FORMAT_STRING} will be used to format the string. The parameters
   * are supplied in the following order: 1 - start epoch, String 2 - operation, String 3 - elapsed
   * time in nanos, Long.
   *
   * @param epochFormatter an instance of {@link EpochFormatter} to format the {@link
   *     OperationMeasurement#epochTimestampNanos}.
   * @return the String representation of the {@link OperationMeasurement}.
   */
  public String toString(@NonNull EpochFormatter epochFormatter) {
    return toString(epochFormatter, DEFAULT_FORMAT_STRING);
  }

  /**
   * Returns the String representation of the {@link OperationMeasurement}.
   *
   * @param epochFormatter an instance of {@link EpochFormatter} to format the {@link
   *     OperationMeasurement#epochTimestampNanos}.
   * @param formatString format string to format the output. The parameters are supplied in the
   *     following order: 1 - start epoch, String 2 - operation, String 3 - elapsed time in nanos,
   *     Long.
   * @return the String representation of the {@link OperationMeasurement}.
   */
  public String toString(@NonNull EpochFormatter epochFormatter, @NonNull String formatString) {
    String result =
        String.format(
            formatString,
            epochFormatter.formatNanos(this.getEpochTimestampNanos()),
            this.succeeded() ? SUCCESS : FAILURE,
            this.getOperation(),
            this.getElapsedTimeNanos());

    if (this.getError().isPresent()) {
      result +=
          String.format(
              DEFAULT_ERROR_FORMAT_STRING,
              this.getError().get().getClass().getCanonicalName(),
              this.getError().get().getMessage());
    }
    return result;
  }

  /**
   * Creates a new {@link OperationMeasurementBuilder}.
   *
   * @return a new instance of {@link OperationMeasurementBuilder}.
   */
  public static OperationMeasurementBuilder builder() {
    return new OperationMeasurementBuilder();
  }

  /**
   * Returns `true` if the operation succeeded, `false` otherwise.
   *
   * @return `true` if the operation succeeded, `false` otherwise.
   */
  public boolean succeeded() {
    return !error.isPresent();
  }

  /**
   * Returns `false` if the operation succeeded, `true` otherwise.
   *
   * @return `false` if the operation succeeded, `true` otherwise.
   */
  public boolean failed() {
    return error.isPresent();
  }

  /**
   * Returns operation elapsed time in nanoseconds.
   *
   * @return operation elapsed time in nanoseconds.
   */
  public long getElapsedTimeNanos() {
    return elapsedCompleteTimeNanos - elapsedStartTimeNanos;
  }

  /** Builder for {@link OperationMeasurement} */
  public static class OperationMeasurementBuilder {
    private static final long UNSET_NANOS = -1L;
    private Operation operation;
    private long epochTimestampNanos = UNSET_NANOS;
    private long elapsedStartTimeNanos = UNSET_NANOS;
    private long elapsedCompleteTimeNanos = UNSET_NANOS;
    private Optional<Throwable> error = Optional.empty();

    /**
     * Sets operation.
     *
     * @param operation operation.
     * @return the current instance of {@link OperationMeasurementBuilder}.
     */
    public OperationMeasurementBuilder operation(@NonNull Operation operation) {
      this.operation = operation;
      return this;
    }

    /**
     * Sets epoch timestamp.
     *
     * @param epochTimestampNanos epoch timestamp.
     * @return the current instance of {@link OperationMeasurementBuilder}.
     */
    public OperationMeasurementBuilder epochTimestampNanos(long epochTimestampNanos) {
      this.epochTimestampNanos = epochTimestampNanos;
      return this;
    }

    /**
     * Sets start time nanos.
     *
     * @param elapsedStartTimeNanos epoch timestamp.
     * @return the current instance of {@link OperationMeasurementBuilder}.
     */
    public OperationMeasurementBuilder elapsedStartTimeNanos(long elapsedStartTimeNanos) {
      this.elapsedStartTimeNanos = elapsedStartTimeNanos;
      return this;
    }

    /**
     * Sets complete time nanos.
     *
     * @param elapsedCompleteTimeNanos epoch timestamp.
     * @return the current instance of {@link OperationMeasurementBuilder}.
     */
    public OperationMeasurementBuilder elapsedCompleteTimeNanos(long elapsedCompleteTimeNanos) {
      this.elapsedCompleteTimeNanos = elapsedCompleteTimeNanos;
      return this;
    }

    /**
     * Sets error.
     *
     * @param error - error.
     * @return the current instance of {@link OperationMeasurementBuilder}.
     */
    public OperationMeasurementBuilder error(Throwable error) {
      this.error = Optional.of(error);
      return this;
    }

    /**
     * Builds the new {@link OperationMeasurement}.
     *
     * @return a new instance of {@link OperationMeasurement}.
     */
    public OperationMeasurement build() {
      Preconditions.checkNotNull(operation, "The `operation` must be set.");
      Preconditions.checkArgument(
          this.epochTimestampNanos >= 0, "The `epochTimestampNanos` must be set and non-negative.");
      Preconditions.checkArgument(
          this.elapsedStartTimeNanos >= 0,
          "The `elapsedStartTimeNanos` must be set and non-negative.");
      Preconditions.checkArgument(
          this.elapsedCompleteTimeNanos >= 0,
          "The `elapsedCompleteTimeNanos` must be set and non-negative.");
      Preconditions.checkArgument(
          this.elapsedCompleteTimeNanos >= elapsedStartTimeNanos,
          "The `elapsedCompleteTimeNanos` must be more or equal than `elapsedStartTimeNanos`.");
      return new OperationMeasurement(
          this.operation,
          this.epochTimestampNanos,
          this.elapsedStartTimeNanos,
          this.elapsedCompleteTimeNanos,
          this.error);
    }
  }
}
