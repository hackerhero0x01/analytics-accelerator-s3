package com.amazon.connector.s3.common.telemetry;

/** Interface that represents a telemetry reporter. */
public interface TelemetryReporter {
  /**
   * Reports telemetry for the operation execution.
   *
   * @param operationMeasurement operation execution.
   */
  void report(OperationMeasurement operationMeasurement);
}
