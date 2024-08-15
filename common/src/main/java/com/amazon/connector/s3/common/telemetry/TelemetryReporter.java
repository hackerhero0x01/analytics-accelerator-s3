package com.amazon.connector.s3.common.telemetry;

/** Interface that represents a telemetry reporter. */
public interface TelemetryReporter {
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
}
