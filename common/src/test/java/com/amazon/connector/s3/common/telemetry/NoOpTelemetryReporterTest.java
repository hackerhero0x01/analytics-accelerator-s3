package com.amazon.connector.s3.common.telemetry;

import org.junit.jupiter.api.Test;

public class NoOpTelemetryReporterTest {
  @Test
  void testCreateAndReport() {
    Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .epochTimestampNanos(1722944779101123456L)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    TelemetryReporter reporter = new NoOpTelemetryReporter();
    reporter.report(operationMeasurement);
  }
}
