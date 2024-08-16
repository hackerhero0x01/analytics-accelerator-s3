package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class TelemetryTest {
  @Test
  void testGetTelemetry() {
    Telemetry newTelemetry = Telemetry.getTelemetry(TelemetryConfiguration.DEFAULT);
    assertNotNull(newTelemetry);
    assertInstanceOf(ConfigurableTelemetry.class, newTelemetry);
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
    DefaultTelemetry defaultTelemetry =
        new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD);

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

    assertEquals(1, reporter.getOperationCompletions().size());
    OperationMeasurement operationMeasurement =
        reporter.getOperationCompletions().stream().findFirst().get();
    assertEquals(operation, operationMeasurement.getOperation());
    assertEquals(10, operationMeasurement.getElapsedStartTimeNanos());
    assertEquals(15, operationMeasurement.getElapsedCompleteTimeNanos());
    assertEquals(5, operationMeasurement.getElapsedTimeNanos());
    assertEquals(5, operationMeasurement.getEpochTimestampNanos());
    assertEquals(Optional.empty(), operationMeasurement.getError());

    // Try again - nothing should be recorded
    result = defaultTelemetry.measureJoin(operation, completableFuture);
    assertEquals(1, reporter.getOperationCompletions().size());
    assertEquals(42, result);
  }

  @Test
  void testMeasureJoinCheckNulls() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    DefaultTelemetry defaultTelemetry =
        new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD);
    CompletableFuture<Long> completableFuture = new CompletableFuture<>();
    Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

    assertThrows(
        NullPointerException.class, () -> defaultTelemetry.measureJoin(null, completableFuture));
    assertThrows(NullPointerException.class, () -> defaultTelemetry.measureJoin(operation, null));
  }
}
