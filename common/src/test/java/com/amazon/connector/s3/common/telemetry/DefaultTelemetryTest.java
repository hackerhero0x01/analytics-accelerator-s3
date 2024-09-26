package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import com.amazon.connector.s3.SpotBugsLambdaWorkaround;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = {"NP_NONNULL_PARAM_VIOLATION", "SIC_INNER_SHOULD_BE_STATIC_ANON"},
    justification = "We mean to pass nulls to checks, and inner classes are appropriate in tests")
public class DefaultTelemetryTest {
  @Test
  void testCreate() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    try (DefaultTelemetry defaultTelemetry =
        new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.CRITICAL)) {

      assertSame(wallClock, defaultTelemetry.getEpochClock());
      assertSame(elapsedClock, defaultTelemetry.getElapsedClock());
      assertSame(reporter, defaultTelemetry.getReporter());
      assertSame(TelemetryLevel.CRITICAL, defaultTelemetry.getLevel());
    }
  }

  @Test
  void testCreateWithNulls() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      SpotBugsLambdaWorkaround.assertThrowsClosableResult(
          NullPointerException.class,
          () -> new DefaultTelemetry(null, elapsedClock, reporter, TelemetryLevel.STANDARD));
      SpotBugsLambdaWorkaround.assertThrowsClosableResult(
          NullPointerException.class,
          () -> new DefaultTelemetry(wallClock, null, reporter, TelemetryLevel.STANDARD));
      SpotBugsLambdaWorkaround.assertThrowsClosableResult(
          NullPointerException.class,
          () -> new DefaultTelemetry(wallClock, elapsedClock, null, TelemetryLevel.STANDARD));
      SpotBugsLambdaWorkaround.assertThrowsClosableResult(
          NullPointerException.class,
          () -> new DefaultTelemetry(wallClock, elapsedClock, reporter, null));
    }
  }

  @Test
  void testFlushAndClose() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    try (DefaultTelemetry defaultTelemetry =
        new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.CRITICAL)) {
      defaultTelemetry.flush();
      assertTrue(reporter.getFlushed().get());
      assertFalse(reporter.getClosed().get());
    }
    assertTrue(reporter.getFlushed().get());
    assertTrue(reporter.getClosed().get());
  }

  @Test
  void testMeasureAction() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    try (DefaultTelemetry defaultTelemetry =
        new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

      Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

      // Tick elapsed clock to 10 and wall clock to 5.
      elapsedClock.tick(10);
      wallClock.tick(5);
      defaultTelemetry.measureStandard(
          () -> operation,
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
  }

  @Test
  void testMeasureMetric() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    try (DefaultTelemetry defaultTelemetry =
        new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

      Metric metric = Metric.builder().name("name").attribute("foo", "bar").build();
      MetricMeasurement metricMeasurement =
          MetricMeasurement.builder().metric(metric).epochTimestampNanos(10L).value(100L).build();

      // Tick elapsed clock to 10 and wall clock to 5.
      elapsedClock.tick(10);
      wallClock.tick(5);
      defaultTelemetry.measure(metricMeasurement);

      assertEquals(1, reporter.getMetrics().size());
      MetricMeasurement metricMeasurementResult = reporter.getMetrics().stream().findFirst().get();
      assertEquals(metricMeasurementResult, metricMeasurement);
    }
  }

  @Test
  void testMeasureActionBelowLevel() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

        // Tick elapsed clock to 10 and wall clock to 5.
        elapsedClock.tick(10);
        wallClock.tick(5);
        defaultTelemetry.measureVerbose(
            () -> operation,
            () -> {
              // This amounts to 5 ns wait.
              elapsedClock.tick(5);
            });

        assertEquals(0, reporter.getOperationCompletions().size());
      }
    }
  }

  @Test
  void testMeasureActionAboveLevel() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

        // Tick elapsed clock to 10 and wall clock to 5.
        elapsedClock.tick(10);
        wallClock.tick(5);
        defaultTelemetry.measureCritical(
            () -> operation,
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
        assertEquals(TelemetryLevel.CRITICAL, operationMeasurement.getLevel());
        assertEquals(Optional.empty(), operationMeasurement.getError());
      }
    }
  }

  @Test
  void testMeasureActionAtLevel() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

        // Tick elapsed clock to 10 and wall clock to 5.
        elapsedClock.tick(10);
        wallClock.tick(5);
        defaultTelemetry.measureStandard(
            () -> operation,
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
        assertEquals(TelemetryLevel.STANDARD, operationMeasurement.getLevel());
        assertEquals(Optional.empty(), operationMeasurement.getError());
      }
    }
  }

  @Test
  void testMeasureActionWithException() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

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
              defaultTelemetry.measureStandard(() -> operation, action);
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
    }
  }

  @Test
  void testMeasureSupplier() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
        Thread result = Thread.currentThread();

        // Tick elapsed clock to 10 and wall clock to 5.
        elapsedClock.tick(10);
        wallClock.tick(5);

        Thread telemetryResult =
            defaultTelemetry.measureStandard(
                () -> operation,
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
    }
  }

  @Test
  void testMeasureSupplierWithBelowLevel() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
        Thread result = Thread.currentThread();

        // Tick elapsed clock to 10 and wall clock to 5.
        elapsedClock.tick(10);
        wallClock.tick(5);

        Thread telemetryResult =
            defaultTelemetry.measureVerbose(
                () -> operation,
                () -> {
                  // This amounts to 5 ns wait.
                  elapsedClock.tick(5);
                  return result;
                });

        assertSame(telemetryResult, result);
        assertEquals(0, reporter.getOperationCompletions().size());
      }
    }
  }

  @Test
  void testMeasureSupplierWithAboveLevel() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
        Thread result = Thread.currentThread();

        // Tick elapsed clock to 10 and wall clock to 5.
        elapsedClock.tick(10);
        wallClock.tick(5);

        Thread telemetryResult =
            defaultTelemetry.measureCritical(
                () -> operation,
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
        assertEquals(TelemetryLevel.CRITICAL, operationMeasurement.getLevel());
        assertEquals(Optional.empty(), operationMeasurement.getError());
      }
    }
  }

  @Test
  void testMeasureSupplierAtLevel() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
        Thread result = Thread.currentThread();

        // Tick elapsed clock to 10 and wall clock to 5.
        elapsedClock.tick(10);
        wallClock.tick(5);

        Thread telemetryResult =
            defaultTelemetry.measureStandard(
                () -> operation,
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
        assertEquals(TelemetryLevel.STANDARD, operationMeasurement.getLevel());
        assertEquals(Optional.empty(), operationMeasurement.getError());
      }
    }
  }

  @Test
  void testMeasureSupplierWithException() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
        Exception error = new IllegalStateException("Error");

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
              defaultTelemetry.measureStandard(() -> operation, supplier);
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
    }
  }

  @Test
  void testMeasureActionThrowingReporterOnStart() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter =
        new CollectingTelemetryReporter() {
          @Override
          public void reportStart(long epochTimestampNanos, Operation operation) {
            throw new IllegalStateException("Error");
          }
        }) {

      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {
        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

        // Tick elapsed clock to 10 and wall clock to 5.
        elapsedClock.tick(10);
        wallClock.tick(5);
        defaultTelemetry.measureStandard(
            () -> operation,
            () -> {
              // This amounts to 5 ns wait.
              elapsedClock.tick(5);
            });

        assertEquals(0, reporter.getOperationStarts().size());
      }
    }
  }

  @Test
  void testMeasureActionThrowingReporterOnComplete() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter =
        new CollectingTelemetryReporter() {
          @Override
          public void reportComplete(TelemetryDatapointMeasurement datapointMeasurement) {
            throw new IllegalStateException("Error");
          }
        }) {

      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {
        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

        // Tick elapsed clock to 10 and wall clock to 5.
        elapsedClock.tick(10);
        wallClock.tick(5);
        defaultTelemetry.measureStandard(
            () -> operation,
            () -> {
              // This amounts to 5 ns wait.
              elapsedClock.tick(5);
            });

        assertEquals(0, reporter.getOperationCompletions().size());
      }
    }
  }

  @Test
  void testMeasureActionNullValues() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {
        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

        TelemetryAction action =
            () -> {
              elapsedClock.tick(5);
            };

        assertThrows(
            NullPointerException.class,
            () -> defaultTelemetry.measure(null, () -> operation, action));
        assertThrows(
            NullPointerException.class, () -> defaultTelemetry.measureStandard(null, action));
        assertThrows(
            NullPointerException.class, () -> defaultTelemetry.measureStandard(() -> null, action));
        assertThrows(
            NullPointerException.class,
            () -> defaultTelemetry.measureStandard(() -> operation, (TelemetryAction) null));
      }
    }
  }

  @Test
  void testMeasureSupplierNullValues() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {
        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

        TelemetrySupplier<Thread> supplier =
            () -> {
              elapsedClock.tick(5);
              return Thread.currentThread();
            };

        assertThrows(
            NullPointerException.class,
            () -> defaultTelemetry.measure(null, () -> operation, supplier));
        assertThrows(
            NullPointerException.class, () -> defaultTelemetry.measureStandard(null, supplier));
        assertThrows(
            NullPointerException.class,
            () -> defaultTelemetry.measureStandard(() -> null, supplier));
        assertThrows(
            NullPointerException.class,
            () ->
                defaultTelemetry.measureStandard(
                    () -> operation, (TelemetrySupplier<Thread>) null));
      }
    }
  }

  @Test
  void testMeasureFuture() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();

        elapsedClock.tick(10);
        wallClock.tick(5);
        CompletableFuture<Long> result =
            defaultTelemetry.measureStandard(() -> operation, completableFuture);
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
    }
  }

  @Test
  void testMeasureFutureBelowLevel() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();

        elapsedClock.tick(10);
        wallClock.tick(5);
        CompletableFuture<Long> result =
            defaultTelemetry.measureVerbose(() -> operation, completableFuture);
        assertFalse(result.isDone());
        assertEquals(0, reporter.getOperationCompletions().size());

        // Tick ahead
        elapsedClock.tick(5);

        // Complete the future
        completableFuture.complete(42L);
        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        assertEquals(42, result.get());

        assertEquals(0, reporter.getOperationCompletions().size());
      }
    }
  }

  @Test
  void testMeasureFutureAboveLevel() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();

        elapsedClock.tick(10);
        wallClock.tick(5);
        CompletableFuture<Long> result =
            defaultTelemetry.measureCritical(() -> operation, completableFuture);
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
        assertEquals(TelemetryLevel.CRITICAL, operationMeasurement.getLevel());
        assertEquals(Optional.empty(), operationMeasurement.getError());
      }
    }
  }

  @Test
  void testMeasureFutureAtLevel() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();

        elapsedClock.tick(10);
        wallClock.tick(5);
        CompletableFuture<Long> result =
            defaultTelemetry.measureStandard(() -> operation, completableFuture);
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
        assertEquals(TelemetryLevel.STANDARD, operationMeasurement.getLevel());
        assertEquals(Optional.empty(), operationMeasurement.getError());
      }
    }
  }

  @Test
  void testMeasureFutureWithException() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      try (DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD)) {

        Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();

        elapsedClock.tick(10);
        wallClock.tick(5);
        CompletableFuture<Long> result =
            defaultTelemetry.measureStandard(() -> operation, completableFuture);
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
    }
  }

  @Test
  void testMeasureFutureWithNulls() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter reporter = new CollectingTelemetryReporter()) {
      DefaultTelemetry defaultTelemetry =
          new DefaultTelemetry(wallClock, elapsedClock, reporter, TelemetryLevel.STANDARD);
      Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
      CompletableFuture<Long> completableFuture = new CompletableFuture<>();

      assertThrows(
          NullPointerException.class,
          () -> defaultTelemetry.measure(null, () -> operation, completableFuture));
      assertThrows(
          NullPointerException.class,
          () -> defaultTelemetry.measureStandard(null, completableFuture));
      assertThrows(
          NullPointerException.class,
          () -> defaultTelemetry.measureStandard(() -> null, completableFuture));

      assertThrows(
          NullPointerException.class,
          () -> defaultTelemetry.measureStandard(() -> operation, (CompletableFuture<Long>) null));
    }
  }
}
