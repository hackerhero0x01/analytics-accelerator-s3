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

import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.s3.dataaccelerator.common.Preconditions;

/** Represents telemetry for the operation measurement. */
@Getter
@EqualsAndHashCode(callSuper = true)
public class OperationMeasurement extends TelemetryDatapointMeasurement {
  /** Telemetry level. * */
  @NonNull private final TelemetryLevel level;
  /** Wall clock time corresponding to operation start. */
  private final long elapsedStartTimeNanos;
  /** Elapsed clock time corresponding to operation completion. */
  private final long elapsedCompleteTimeNanos;
  /** Exception thrown as part of the execution. */
  @NonNull private final Optional<Throwable> error;

  public static final String DEFAULT_START_FORMAT_STRING = "[%s] [  start] %s";
  public static final String DEFAULT_COMPLETE_FORMAT_STRING = "[%s] [%s] %s: %,d ns";
  private static final String DEFAULT_ERROR_FORMAT_STRING = " [%s: '%s']";
  private static final String SUCCESS = "success";
  private static final String FAILURE = "failure";

  /**
   * Creates a new instance of {@link OperationMeasurement}
   *
   * @param operation the underlying {@link Operation}
   * @param level telemetry level
   * @param epochTimestampNanos wall clock time corresponding to operation start
   * @param elapsedStartTimeNanos elapsed clock time corresponding to operation start
   * @param elapsedCompleteTimeNanos elapsed clock time corresponding to operation completion
   * @param error exception thrown as part of the execution
   */
  private OperationMeasurement(
      Operation operation,
      @NonNull TelemetryLevel level,
      long epochTimestampNanos,
      long elapsedStartTimeNanos,
      long elapsedCompleteTimeNanos,
      @NonNull Optional<Throwable> error) {
    super(operation, epochTimestampNanos);
    this.level = level;
    this.elapsedStartTimeNanos = elapsedStartTimeNanos;
    this.elapsedCompleteTimeNanos = elapsedCompleteTimeNanos;
    this.error = error;
  }

  /**
   * The actual measurement
   *
   * @return the actual measurements
   */
  @Override
  protected double getValueCore() {
    return getElapsedTimeNanos();
  }

  /**
   * Returns the underlying {@link Operation}
   *
   * @return underlying {@link Operation}
   */
  public Operation getOperation() {
    return (Operation) this.getDatapoint();
  }

  /**
   * Returns the String representation of the {@link OperationMeasurement}. {@link
   * OperationMeasurement#DEFAULT_COMPLETE_FORMAT_STRING} will be used to format the string. The
   * parameters are supplied in the following order: 1 - start epoch, String 2 - operation, String 3
   * - elapsed time in nanos, Long.
   *
   * @param epochFormatter an instance of {@link EpochFormatter} to format the epochTimestampNanos
   * @return the String representation of the {@link OperationMeasurement}.
   */
  public String toString(@NonNull EpochFormatter epochFormatter) {
    return toString(epochFormatter, DEFAULT_COMPLETE_FORMAT_STRING);
  }

  /**
   * Returns the String representation of the {@link OperationMeasurement}.
   *
   * @param epochFormatter an instance of {@link EpochFormatter} to format epochTimestampNanos.
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

  /**
   * Returns the String representation of the {@link Operation} and epochTimestampNanos. This is
   * used to format the "operation starting" message.
   *
   * @param operation {@link Operation} that is starting operation.
   * @param epochTimestampNanos wall clock epoch of the operation start, in nanos.
   * @return formatted string.
   */
  public static String getOperationStartingString(Operation operation, long epochTimestampNanos) {
    return getOperationStartingString(operation, epochTimestampNanos, DEFAULT_START_FORMAT_STRING);
  }

  /**
   * Returns the String representation of the {@link Operation} and epochTimestampNanos. This is
   * used to format the "operation starting" message.
   *
   * @param operation {@link Operation} that is starting operation.
   * @param epochTimestampNanos wall clock epoch of the operation start, in nanos.
   * @param formatString format string to use. The parameters are supplied in the following order: 1
   *     - start epoch, String 2 - operation.
   * @return formatted string.
   */
  public static String getOperationStartingString(
      Operation operation, long epochTimestampNanos, String formatString) {
    return getOperationStartingString(
        operation, epochTimestampNanos, EpochFormatter.DEFAULT, formatString);
  }

  /**
   * Returns the String representation of the {@link Operation} and epochTimestampNanos. This is
   * used to format the "operation starting" message.
   *
   * @param operation {@link Operation} that is starting operation.
   * @param epochTimestampNanos wall clock epoch of the operation start, in nanos.
   * @param epochFormatter {@link EpochFormatter} to use to format epochTimestampNanos
   * @return formatted string.
   */
  public static String getOperationStartingString(
      Operation operation, long epochTimestampNanos, EpochFormatter epochFormatter) {
    return getOperationStartingString(
        operation, epochTimestampNanos, epochFormatter, DEFAULT_START_FORMAT_STRING);
  }

  /**
   * Returns the String representation of the {@link Operation} and epochTimestampNanos. This is
   * used to format the "operation starting" message.
   *
   * @param operation {@link Operation} that is starting operation.
   * @param epochTimestampNanos wall clock epoch of the operation start, in nanos.
   * @param epochFormatter {@link EpochFormatter} to use to format epochTimestampNanos
   * @param formatString format string to use.The parameters are supplied in the following order: 1
   *     - start epoch, String 2 - operation.
   * @return formatted string.
   */
  public static String getOperationStartingString(
      @NonNull Operation operation,
      long epochTimestampNanos,
      @NonNull EpochFormatter epochFormatter,
      @NonNull String formatString) {
    return String.format(formatString, epochFormatter.formatNanos(epochTimestampNanos), operation);
  }

  /** Builder for {@link OperationMeasurement} */
  public static class OperationMeasurementBuilder
      extends TelemetryDatapointMeasurementBuilder<
          OperationMeasurement, OperationMeasurementBuilder> {
    private Operation operation;
    TelemetryLevel level;
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
     * Sets operation.
     *
     * @param level level.
     * @return the current instance of {@link OperationMeasurementBuilder}.
     */
    public OperationMeasurementBuilder level(@NonNull TelemetryLevel level) {
      this.level = level;
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
    @Override
    protected OperationMeasurement buildCore() {
      Preconditions.checkNotNull(operation, "The `operation` must be set.");
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
          this.level,
          this.getEpochTimestampNanos(),
          this.elapsedStartTimeNanos,
          this.elapsedCompleteTimeNanos,
          this.error);
    }
  }
}
