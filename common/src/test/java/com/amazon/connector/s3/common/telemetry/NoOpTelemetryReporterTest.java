package com.amazon.connector.s3.common.telemetry;

import org.junit.jupiter.api.Test;

public class NoOpTelemetryReporterTest {
  private static final long TEST_EPOCH_NANOS = 1722944779101123456L;

  @Test
  void testCreateAndReportComplete() {
    Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .level(TelemetryLevel.STANDARD)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    try (TelemetryReporter reporter = new NoOpTelemetryReporter()) {
      reporter.reportComplete(operationMeasurement);
    }
  }
}
