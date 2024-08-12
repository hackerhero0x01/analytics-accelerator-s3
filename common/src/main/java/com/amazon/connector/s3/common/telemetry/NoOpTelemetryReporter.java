package com.amazon.connector.s3.common.telemetry;

/** a {@link TelemetryReporter} that does nothing. */
class NoOpTelemetryReporter implements TelemetryReporter {
  @Override
  public void report(OperationMeasurement operationMeasurement) {}
}
