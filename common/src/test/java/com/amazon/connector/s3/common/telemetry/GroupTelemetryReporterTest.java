package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class GroupTelemetryReporterTest {
  private static final long TEST_EPOCH_NANOS = 1722944779101123456L;

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
  void testReportComplete() {
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
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    reporter.reportComplete(operationMeasurement);
    reporters.forEach(
        r ->
            assertArrayEquals(
                ((CollectingTelemetryReporter) r).getOperationCompletions().toArray(),
                new OperationMeasurement[] {operationMeasurement}));
  }

  @Test
  void testReportStart() {
    CollectingTelemetryReporter reporter1 = new CollectingTelemetryReporter();
    CollectingTelemetryReporter reporter2 = new CollectingTelemetryReporter();
    List<TelemetryReporter> reporters = new ArrayList<>();
    reporters.add(reporter1);
    reporters.add(reporter2);
    GroupTelemetryReporter reporter = new GroupTelemetryReporter(reporters);

    Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
    reporter.reportStart(TEST_EPOCH_NANOS, operation);

    reporters.forEach(
        r ->
            assertArrayEquals(
                ((CollectingTelemetryReporter) r).getOperationStarts().toArray(),
                new Operation[] {operation}));
  }
}
