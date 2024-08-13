package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

public class TelemetryTest {
  @Test
  void testCreateTelemetry() {
    Telemetry newTelemetry = Telemetry.getTelemetry(TelemetryConfiguration.DEFAULT);
    assertInstanceOf(ConfigurableTelemetry.class, newTelemetry);
    ConfigurableTelemetry telemetry = (ConfigurableTelemetry) newTelemetry;

    assertInstanceOf(GroupTelemetryReporter.class, telemetry.getReporter());
    GroupTelemetryReporter groupTelemetryReporter =
        (GroupTelemetryReporter) telemetry.getReporter();
    assertEquals(2, groupTelemetryReporter.getReporters().size());
    TelemetryReporter[] telemetryReporters = new TelemetryReporter[2];
    groupTelemetryReporter.getReporters().toArray(telemetryReporters);

    assertInstanceOf(PrintStreamTelemetryReporter.class, telemetryReporters[0]);
    PrintStreamTelemetryReporter printStreamTelemetryReporter =
        (PrintStreamTelemetryReporter) telemetryReporters[0];
    assertSame(printStreamTelemetryReporter.getEpochFormatter(), EpochFormatter.DEFAULT);
    assertSame(printStreamTelemetryReporter.getPrintStream(), System.out);

    assertInstanceOf(LoggingTelemetryReporter.class, telemetryReporters[1]);
    LoggingTelemetryReporter loggingTelemetryReporter =
        (LoggingTelemetryReporter) telemetryReporters[1];
    assertSame(loggingTelemetryReporter.getEpochFormatter(), EpochFormatter.DEFAULT);
    assertEquals(loggingTelemetryReporter.getLoggerLevel(), Level.INFO);
    assertEquals(
        loggingTelemetryReporter.getLoggerName(), LoggingTelemetryReporter.DEFAULT_LOGGER_NAME);
  }

  @Test
  void testCreateTelemetryWithNulls() {
    assertThrows(NullPointerException.class, () -> Telemetry.getTelemetry(null));
  }

  @Test
  void testNoOp() {
    Telemetry noopTelemetry = Telemetry.NOOP;
    assertInstanceOf(DefaultTelemetry.class, noopTelemetry);
    DefaultTelemetry telemetry = (DefaultTelemetry) noopTelemetry;
    assertInstanceOf(NoOpTelemetryReporter.class, telemetry.getReporter());
  }

  @Test
  void testMeasureJoin() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    DefaultTelemetry defaultTelemetry = new DefaultTelemetry(wallClock, elapsedClock, reporter);

    Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
    final CompletableFuture<Long> completableFuture = new CompletableFuture<>();
    Thread completionThread =
        new Thread(
            () -> {
              try {
                Thread.sleep(5_000);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              elapsedClock.tick(5);
              completableFuture.complete(42L);
            });

    elapsedClock.tick(10);
    wallClock.tick(5);

    // This will complete the future
    completionThread.start();
    Long result = defaultTelemetry.measureJoin(operation, completableFuture);
    assertTrue(completableFuture.isDone());
    assertFalse(completableFuture.isCompletedExceptionally());
    assertEquals(42, completableFuture.get());

    assertEquals(1, reporter.getOperationMeasurements().size());
    OperationMeasurement operationMeasurement =
        reporter.getOperationMeasurements().stream().findFirst().get();
    assertEquals(operation, operationMeasurement.getOperation());
    assertEquals(10, operationMeasurement.getElapsedStartTimeNanos());
    assertEquals(15, operationMeasurement.getElapsedCompleteTimeNanos());
    assertEquals(5, operationMeasurement.getElapsedTimeNanos());
    assertEquals(5, operationMeasurement.getEpochTimestampNanos());
    assertEquals(Optional.empty(), operationMeasurement.getError());

    // Try again - nothing should be recorded
    result = defaultTelemetry.measureJoin(operation, completableFuture);
    assertEquals(1, reporter.getOperationMeasurements().size());
    assertEquals(42, result);
  }

  @Test
  void testMeasureJoinCheckNulls() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    DefaultTelemetry defaultTelemetry = new DefaultTelemetry(wallClock, elapsedClock, reporter);
    CompletableFuture<Long> completableFuture = new CompletableFuture<>();
    Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();


    assertThrows(NullPointerException.class, () -> defaultTelemetry.measureJoin(null, completableFuture));
    assertThrows(NullPointerException.class, () -> defaultTelemetry.measureJoin(operation, null));
  }
}
