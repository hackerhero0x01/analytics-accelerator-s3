package com.amazon.connector.s3.common.telemetry;

import java.io.Closeable;

/** Interface that represents a telemetry reporter. */
public interface TelemetryReporter extends Closeable {
  /**
   * Reports the start of an operation
   *
   * @param epochTimestampNanos wall clock time for the operation start
   * @param operation and instance of {@link Operation} to start
   */
  void reportStart(long epochTimestampNanos, Operation operation);

  /**
   * Reports the completion of an operation
   *
   * @param operationMeasurement an instance of {@link OperationMeasurement}.
   */
  void reportComplete(OperationMeasurement operationMeasurement);

  /** Flushes any intermediate state of the reporters */
  void flush();

  /**
   * Default implementation of {@link AutoCloseable#close()}, that calls {@link
   * TelemetryReporter#flush()}
   */
  default void close() {
    this.flush();
  }
}
