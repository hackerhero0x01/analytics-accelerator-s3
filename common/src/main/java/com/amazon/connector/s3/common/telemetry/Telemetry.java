package com.amazon.connector.s3.common.telemetry;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.NonNull;

/** This is a set of operations that support adding telemetry for operation execution. */
public interface Telemetry {
  /**
   * Measures a given {@link Runnable} and record the telemetry as {@link Operation}.
   *
   * @param operation operation to record this execution as.
   * @param operationCode - code to execute.
   */
  void measure(@NonNull Operation operation, @NonNull TelemetryAction operationCode);

  /**
   * Measures a given {@link Supplier <T>} and record the telemetry as {@link Operation}.
   *
   * @param operation operation to record this execution as.
   * @param operationCode code to execute.
   * @param <T> return type of the {@link Supplier<T>}.
   * @return the value that {@link Supplier<T>} returns.
   */
  <T> T measure(@NonNull Operation operation, @NonNull TelemetrySupplier<T> operationCode);

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
  <T> CompletableFuture<T> measure(
      @NonNull Operation operation, @NonNull CompletableFuture<T> operationCode);

  /**
   * This is a helper method to reduce verbosity on completed futures. Blocks on the execution on
   * {@link CompletableFuture#join()} and records the telemetry as {@link Operation}. We do not
   * currently carry the operation into the context of any continuations, so any {@link Operation}s
   * that are created in that context need to carry the parenting chain. The telemetry is only
   * recorded if the future was not completed, which is checked via {@link
   * CompletableFuture#isDone()}
   *
   * @param operation operation to record this execution as.
   * @param operationCode the future to measure the execution of.
   * @return an instance of {@link T} that returns the same result as the one passed in.
   * @param <T> - return type of the {@link CompletableFuture<T>}.
   */
  default <T> T measureJoin(
      @NonNull Operation operation, @NonNull CompletableFuture<T> operationCode) {
    if (operationCode.isDone()) {
      return operationCode.join();
    } else {
      return this.measure(operation, operationCode::join);
    }
  }

  /**
   * Creates a new instance of {@link Telemetry} based on the configuration.
   *
   * @param configuration an instance of {@link TelemetryConfiguration}.
   * @return a new instance of {@link Telemetry}, as defined by the configuration.
   */
  static Telemetry getTelemetry(@NonNull TelemetryConfiguration configuration) {
    return new ConfigurableTelemetry(configuration);
  }

  /** An instance of {@link Telemetry} that reports nothing. */
  static Telemetry NOOP =
      new DefaultTelemetry(
          DefaultEpochClock.DEFAULT,
          DefaultElapsedClock.DEFAULT,
          new NoOpTelemetryReporter(),
          TelemetryLevel.CRITICAL);
}
