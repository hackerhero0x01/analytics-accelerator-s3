package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class GroupTelemetryReporterTest {
  @Test
  void testCreate() {
    CollectingTelemetryReporter reporter1 = new CollectingTelemetryReporter();
    CollectingTelemetryReporter reporter2 = new CollectingTelemetryReporter();
    List<TelemetryReporter> reporters = new ArrayList<>();
    reporters.add(reporter1);
    reporters.add(reporter2);
    GroupTelemetryReporter reporter = new GroupTelemetryReporter(reporters);
    assertArrayEquals(reporter.getReporters().toArray(), reporters.toArray());
  }

  @Test
  void testCreateWithNulls() {
    assertThrows(NullPointerException.class, () -> new GroupTelemetryReporter(null));
  }

  @Test
  void testReport() {
    CollectingTelemetryReporter reporter1 = new CollectingTelemetryReporter();
    CollectingTelemetryReporter reporter2 = new CollectingTelemetryReporter();
    List<TelemetryReporter> reporters = new ArrayList<>();
    reporters.add(reporter1);
    reporters.add(reporter2);
    GroupTelemetryReporter reporter = new GroupTelemetryReporter(reporters);

    Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .epochTimestampNanos(1722944779101123456L)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    reporter.report(operationMeasurement);
    reporters.forEach(
        r ->
            assertArrayEquals(
                ((CollectingTelemetryReporter) r).getOperationMeasurements().toArray(),
                new OperationMeasurement[] {operationMeasurement}));
  }
}
