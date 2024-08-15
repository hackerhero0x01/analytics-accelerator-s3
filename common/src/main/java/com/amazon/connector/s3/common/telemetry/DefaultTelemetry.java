package com.amazon.connector.s3.common.telemetry;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** This is a set of operations that support adding telemetry for operation execution. */
@Getter
@RequiredArgsConstructor
public class DefaultTelemetry implements Telemetry {
  /** Epoch clock. Used to measure the wall time for {@link Operation} start. */
  @NonNull @Getter(AccessLevel.PACKAGE)
  private final Clock epochClock;
  /** Elapsed clock. Used to measure the duration for {@link Operation}. */
  @NonNull @Getter(AccessLevel.PACKAGE)
  private final Clock elapsedClock;

  @NonNull @Getter(AccessLevel.PACKAGE)
  private final TelemetryReporter reporter;

  private static final Logger LOG = LogManager.getLogger(DefaultTelemetry.class);

  /**
   * Executes a given {@link Runnable} and record the telemetry as {@link Operation}.
   *
   * @param operation operation to record this execution as.
   * @param operationCode code to execute.
   */
  @SneakyThrows
  public void measure(@NonNull Operation operation, @NonNull TelemetryAction operationCode) {
    OperationMeasurement.OperationMeasurementBuilder builder = startMeasurement(operation);
    try {
      operation.getContext().pushOperation(operation);
      operationCode.apply();
      completeMeasurement(builder, Optional.empty());
    } catch (Exception error) {
      completeMeasurement(builder, Optional.of(error));
      throw error;
    } finally {
      operation.getContext().popOperation(operation);
    }
  }

  /**
   * Executes a given {@link Supplier<T>} and records the telemetry as {@link Operation}.
   *
   * @param operation operation to record this execution as.
   * @param operationCode code to execute.
   * @param <T> return type of the {@link Supplier<T>}.
   * @return the value that {@link Supplier<T>} returns.
   */
  @SneakyThrows
  public <T> T measure(@NonNull Operation operation, @NonNull TelemetrySupplier<T> operationCode) {
    OperationMeasurement.OperationMeasurementBuilder builder = startMeasurement(operation);
    try {
      operation.getContext().pushOperation(operation);
      T result = operationCode.apply();
      completeMeasurement(builder, Optional.empty());
      return result;
    } catch (Throwable error) {
      completeMeasurement(builder, Optional.of(error));
      throw error;
    } finally {
      operation.getContext().popOperation(operation);
    }
  }

  /**
   * Measures the execution of the given {@link CompletableFuture} and records the telemetry as
   * {@link Operation}. We do not currently carry the operation into the context of any
   * continuations, so any {@link Operation}s that are created in that context need to carry the
   * parenting chain.
   *
   * @param operation operation to record this execution as.
   * @param operationCode the future to measure the execution of.
   * @return an instance of {@link CompletableFuture} that returns the same result as the one passed
   *     in.
   * @param <T> - return type of the {@link CompletableFuture<T>}.
   */
  @SneakyThrows
  public <T> CompletableFuture<T> measure(
      @NonNull Operation operation, CompletableFuture<T> operationCode) {
    OperationMeasurement.OperationMeasurementBuilder builder = startMeasurement(operation);
    operationCode.whenComplete(
        (result, error) -> completeMeasurement(builder, Optional.ofNullable(error)));
    return operationCode;
  }

  /**
   * Does all the bookkeeping at the operation starts.
   *
   * @param operation operation being executed.
   * @return {@link OperationMeasurement.OperationMeasurementBuilder} with all the necessary state.
   */
  private OperationMeasurement.OperationMeasurementBuilder startMeasurement(Operation operation) {
    // Create the builder
    OperationMeasurement.OperationMeasurementBuilder builder = OperationMeasurement.builder();

    // Record start times
    long epochTimestampNanos = epochClock.getCurrentTimeNanos();
    builder.operation(operation);
    builder.epochTimestampNanos(epochTimestampNanos);
    builder.elapsedStartTimeNanos(elapsedClock.getCurrentTimeNanos());

    this.recordOperationStart(epochTimestampNanos, operation);

    return builder;
  }

  /**
   * Does all the bookkeeping at the end of the operation and returns the {@link
   * OperationMeasurement} and records the execution to the reporter.
   *
   * @param builder {@link OperationMeasurement.OperationMeasurementBuilder} representing the
   *     execution state.
   * @param error error produced during execution, if any.
   */
  private void completeMeasurement(
      OperationMeasurement.OperationMeasurementBuilder builder, Optional<Throwable> error) {
    builder.elapsedCompleteTimeNanos(elapsedClock.getCurrentTimeNanos());
    error.ifPresent(builder::error);
    recordOperationCompletion(builder.build());
  }

  /**
   * Records operation completion.
   *
   * @param operationMeasurement an instance of {@link OperationMeasurement}.
   */
  private void recordOperationCompletion(OperationMeasurement operationMeasurement) {
    try {
      this.reporter.reportComplete(operationMeasurement);
    } catch (Throwable error) {
      LOG.error(
          String.format(
              "Unexpected error reporting operation completion of `%s`.",
              operationMeasurement.getOperation().toString()),
          error);
    }
  }

  /**
   * Records operation start.
   *
   * @param epochTimestampNanos wall clock epoch time of operation start.
   * @param operation an instance of {@link Operation}.
   */
  private void recordOperationStart(long epochTimestampNanos, Operation operation) {
    try {
      this.reporter.reportStart(epochTimestampNanos, operation);
    } catch (Throwable error) {
      LOG.error(
          String.format(
              "Unexpected error reporting operation start of `%s`.", operation.toString()),
          error);
    }
  }
}
