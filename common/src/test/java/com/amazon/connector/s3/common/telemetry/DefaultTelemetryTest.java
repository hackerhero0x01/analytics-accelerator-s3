package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class DefaultTelemetryTest {
  @Test
  void testCreate() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    DefaultTelemetry defaultTelemetry = new DefaultTelemetry(wallClock, elapsedClock, reporter);

    assertSame(wallClock, defaultTelemetry.getEpochClock());
    assertSame(elapsedClock, defaultTelemetry.getElapsedClock());
    assertSame(reporter, defaultTelemetry.getReporter());
  }

  @Test
  void testCreateWithNulls() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    assertThrows(
        NullPointerException.class, () -> new DefaultTelemetry(null, elapsedClock, reporter));
    assertThrows(NullPointerException.class, () -> new DefaultTelemetry(wallClock, null, reporter));
    assertThrows(
        NullPointerException.class, () -> new DefaultTelemetry(wallClock, elapsedClock, null));
  }

  @Test
  void testMeasureAction() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    DefaultTelemetry defaultTelemetry = new DefaultTelemetry(wallClock, elapsedClock, reporter);

    Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

    // Tick elapsed clock to 10 and wall clock to 5.
    elapsedClock.tick(10);
    wallClock.tick(5);
    defaultTelemetry.measure(
        operation,
        () -> {
          // This amounts to 5 ns wait.
          elapsedClock.tick(5);
        });

    assertEquals(1, reporter.getOperationCompletions().size());
    OperationMeasurement operationMeasurement =
        reporter.getOperationCompletions().stream().findFirst().get();
    assertEquals(operation, operationMeasurement.getOperation());
    assertEquals(10, operationMeasurement.getElapsedStartTimeNanos());
    assertEquals(15, operationMeasurement.getElapsedCompleteTimeNanos());
    assertEquals(5, operationMeasurement.getElapsedTimeNanos());
    assertEquals(5, operationMeasurement.getEpochTimestampNanos());
    assertEquals(Optional.empty(), operationMeasurement.getError());
  }

  @Test
  void testMeasureActionWithException() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    DefaultTelemetry defaultTelemetry = new DefaultTelemetry(wallClock, elapsedClock, reporter);

    Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
    Exception error = new IllegalStateException("Error");

    TelemetryAction action =
        () -> {
          elapsedClock.tick(5);
          throw error;
        };
    // Tick elapsed clock to 10 and wall clock to 5.
    elapsedClock.tick(10);
    wallClock.tick(5);
    assertThrows(
        IllegalStateException.class,
        () -> {
          defaultTelemetry.measure(operation, action);
        });

    assertEquals(1, reporter.getOperationCompletions().size());
    OperationMeasurement operationMeasurement =
        reporter.getOperationCompletions().stream().findFirst().get();
    assertEquals(operation, operationMeasurement.getOperation());
    assertEquals(10, operationMeasurement.getElapsedStartTimeNanos());
    assertEquals(15, operationMeasurement.getElapsedCompleteTimeNanos());
    assertEquals(5, operationMeasurement.getElapsedTimeNanos());
    assertEquals(5, operationMeasurement.getEpochTimestampNanos());
    assertSame(error, operationMeasurement.getError().get());
  }

  @Test
  void testMeasureSupplier() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    DefaultTelemetry defaultTelemetry = new DefaultTelemetry(wallClock, elapsedClock, reporter);

    Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
    Thread result = Thread.currentThread();

    // Tick elapsed clock to 10 and wall clock to 5.
    elapsedClock.tick(10);
    wallClock.tick(5);

    Thread telemetryResult =
        defaultTelemetry.measure(
            operation,
            () -> {
              // This amounts to 5 ns wait.
              elapsedClock.tick(5);
              return result;
            });

    assertSame(telemetryResult, result);
    assertEquals(1, reporter.getOperationCompletions().size());
    OperationMeasurement operationMeasurement =
        reporter.getOperationCompletions().stream().findFirst().get();
    assertEquals(operation, operationMeasurement.getOperation());
    assertEquals(10, operationMeasurement.getElapsedStartTimeNanos());
    assertEquals(15, operationMeasurement.getElapsedCompleteTimeNanos());
    assertEquals(5, operationMeasurement.getElapsedTimeNanos());
    assertEquals(5, operationMeasurement.getEpochTimestampNanos());
    assertEquals(Optional.empty(), operationMeasurement.getError());
  }

  @Test
  void testMeasureSupplierWithException() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    DefaultTelemetry defaultTelemetry = new DefaultTelemetry(wallClock, elapsedClock, reporter);

    Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
    Exception error = new IllegalStateException("Error");
    Thread result = Thread.currentThread();

    TelemetrySupplier<Thread> supplier =
        () -> {
          elapsedClock.tick(5);
          throw error;
        };

    // Tick elapsed clock to 10 and wall clock to 5.
    elapsedClock.tick(10);
    wallClock.tick(5);
    assertThrows(
        IllegalStateException.class,
        () -> {
          defaultTelemetry.measure(operation, supplier);
        });

    assertEquals(1, reporter.getOperationCompletions().size());
    OperationMeasurement operationMeasurement =
        reporter.getOperationCompletions().stream().findFirst().get();
    assertEquals(operation, operationMeasurement.getOperation());
    assertEquals(10, operationMeasurement.getElapsedStartTimeNanos());
    assertEquals(15, operationMeasurement.getElapsedCompleteTimeNanos());
    assertEquals(5, operationMeasurement.getElapsedTimeNanos());
    assertEquals(5, operationMeasurement.getEpochTimestampNanos());
    assertSame(error, operationMeasurement.getError().get());
  }

  @Test
  void testMeasureActionThrowingReporterOnStart() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter =
        new CollectingTelemetryReporter() {
          @Override
          public void reportStart(long epochTimestampNanos, Operation operation) {
            throw new IllegalStateException("Error");
          }
        };

    DefaultTelemetry defaultTelemetry = new DefaultTelemetry(wallClock, elapsedClock, reporter);
    Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

    // Tick elapsed clock to 10 and wall clock to 5.
    elapsedClock.tick(10);
    wallClock.tick(5);
    defaultTelemetry.measure(
        operation,
        () -> {
          // This amounts to 5 ns wait.
          elapsedClock.tick(5);
        });

    assertEquals(0, reporter.getOperationStarts().size());
  }

  @Test
  void testMeasureActionThrowingReporterOnComplete() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter =
        new CollectingTelemetryReporter() {
          @Override
          public void reportComplete(OperationMeasurement operationMeasurement) {
            throw new IllegalStateException("Error");
          }
        };

    DefaultTelemetry defaultTelemetry = new DefaultTelemetry(wallClock, elapsedClock, reporter);
    Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

    // Tick elapsed clock to 10 and wall clock to 5.
    elapsedClock.tick(10);
    wallClock.tick(5);
    defaultTelemetry.measure(
        operation,
        () -> {
          // This amounts to 5 ns wait.
          elapsedClock.tick(5);
        });

    assertEquals(0, reporter.getOperationCompletions().size());
  }

  @Test
  void testMeasureActionNullValues() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    DefaultTelemetry defaultTelemetry = new DefaultTelemetry(wallClock, elapsedClock, reporter);
    Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

    TelemetryAction action =
        () -> {
          elapsedClock.tick(5);
        };

    assertThrows(NullPointerException.class, () -> defaultTelemetry.measure(null, action));
    assertThrows(
        NullPointerException.class,
        () -> defaultTelemetry.measure(operation, (TelemetryAction) null));
  }

  @Test
  void testMeasureSupplierNullValues() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    DefaultTelemetry defaultTelemetry = new DefaultTelemetry(wallClock, elapsedClock, reporter);
    Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

    TelemetrySupplier<Thread> supplier =
        () -> {
          elapsedClock.tick(5);
          return Thread.currentThread();
        };

    assertThrows(NullPointerException.class, () -> defaultTelemetry.measure(null, supplier));
    assertThrows(
        NullPointerException.class,
        () -> defaultTelemetry.measure(operation, (TelemetrySupplier<Thread>) null));
  }

  @Test
  void testMeasureFuture() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    DefaultTelemetry defaultTelemetry = new DefaultTelemetry(wallClock, elapsedClock, reporter);

    Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
    CompletableFuture<Long> completableFuture = new CompletableFuture<>();

    elapsedClock.tick(10);
    wallClock.tick(5);
    CompletableFuture<Long> result = defaultTelemetry.measure(operation, completableFuture);
    assertFalse(result.isDone());
    assertEquals(0, reporter.getOperationCompletions().size());

    // Tick ahead
    elapsedClock.tick(5);

    // Complete the future
    completableFuture.complete(42L);
    assertTrue(result.isDone());
    assertFalse(result.isCompletedExceptionally());
    assertEquals(42, result.get());

    assertEquals(1, reporter.getOperationCompletions().size());
    OperationMeasurement operationMeasurement =
        reporter.getOperationCompletions().stream().findFirst().get();
    assertEquals(operation, operationMeasurement.getOperation());
    assertEquals(10, operationMeasurement.getElapsedStartTimeNanos());
    assertEquals(15, operationMeasurement.getElapsedCompleteTimeNanos());
    assertEquals(5, operationMeasurement.getElapsedTimeNanos());
    assertEquals(5, operationMeasurement.getEpochTimestampNanos());
    assertEquals(Optional.empty(), operationMeasurement.getError());
  }

  @Test
  void testMeasureFutureWithException() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    DefaultTelemetry defaultTelemetry = new DefaultTelemetry(wallClock, elapsedClock, reporter);

    Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
    CompletableFuture<Long> completableFuture = new CompletableFuture<>();

    elapsedClock.tick(10);
    wallClock.tick(5);
    CompletableFuture<Long> result = defaultTelemetry.measure(operation, completableFuture);
    assertFalse(result.isDone());
    assertEquals(0, reporter.getOperationCompletions().size());

    // Tick ahead
    elapsedClock.tick(5);

    // Complete the future

    Exception error = new IllegalStateException("error");
    completableFuture.completeExceptionally(error);

    assertTrue(result.isDone());
    assertTrue(result.isCompletedExceptionally());

    assertEquals(1, reporter.getOperationCompletions().size());
    OperationMeasurement operationMeasurement =
        reporter.getOperationCompletions().stream().findFirst().get();
    assertEquals(operation, operationMeasurement.getOperation());
    assertEquals(10, operationMeasurement.getElapsedStartTimeNanos());
    assertEquals(15, operationMeasurement.getElapsedCompleteTimeNanos());
    assertEquals(5, operationMeasurement.getElapsedTimeNanos());
    assertEquals(5, operationMeasurement.getEpochTimestampNanos());
    assertTrue(operationMeasurement.getError().isPresent());
    assertEquals(error, operationMeasurement.getError().get());
  }

  @Test
  void testMeasureFutureWithNulls() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    DefaultTelemetry defaultTelemetry = new DefaultTelemetry(wallClock, elapsedClock, reporter);
    Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
    CompletableFuture<Long> completableFuture = new CompletableFuture<>();

    assertThrows(
        NullPointerException.class, () -> defaultTelemetry.measure(null, completableFuture));
    assertThrows(
        NullPointerException.class,
        () -> defaultTelemetry.measure(operation, (CompletableFuture<Long>) null));
  }
}
