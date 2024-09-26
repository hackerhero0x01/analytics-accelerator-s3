package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.amazon.connector.s3.SpotBugsLambdaWorkaround;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class TelemetryDatapointAggregatorTest {
  @Test
  void testCreate() {
    TelemetryReporter telemetryReporter = mock(TelemetryReporter.class);
    Clock clock = mock(Clock.class);
    TelemetryDatapointAggregator aggregator =
        new TelemetryDatapointAggregator(telemetryReporter, clock);
    assertSame(telemetryReporter, aggregator.getTelemetryReporter());
    assertSame(clock, aggregator.getEpochClock());
  }

  @Test
  void testCreateWithNulls() {
    TelemetryReporter telemetryReporter = mock(TelemetryReporter.class);
    Clock clock = mock(Clock.class);
    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class, () -> new TelemetryDatapointAggregator(null, clock));
    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class,
        () -> new TelemetryDatapointAggregator(telemetryReporter, null));
  }

  @Test
  void testReportStartDoesNothing() {
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter telemetryReporter = new CollectingTelemetryReporter()) {
      try (TelemetryDatapointAggregator aggregator =
          new TelemetryDatapointAggregator(telemetryReporter, elapsedClock)) {
        Operation operation = Operation.builder().name("Foo").attribute("X", "Y").build();

        aggregator.reportStart(42L, operation);
        aggregator.flush();

        assertTrue(telemetryReporter.getDatapointCompletions().isEmpty());
      }
    }
  }

  @Test
  void testReportMetricShouldProduceAggregation() {
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter telemetryReporter = new CollectingTelemetryReporter()) {
      try (TelemetryDatapointAggregator aggregator =
          new TelemetryDatapointAggregator(telemetryReporter, elapsedClock)) {
        Metric metric = Metric.builder().name("Foo").attribute("X", "Y").build();

        elapsedClock.tick(10L);
        // Produce metrics
        aggregator.reportComplete(
            MetricMeasurement.builder().metric(metric).value(1).epochTimestampNanos(1).build());
        aggregator.reportComplete(
            MetricMeasurement.builder().metric(metric).value(2).epochTimestampNanos(1).build());
        aggregator.reportComplete(
            MetricMeasurement.builder().metric(metric).value(9).epochTimestampNanos(1).build());

        // Nothing should happen yet
        assertTrue(telemetryReporter.getMetrics().isEmpty());

        // Now flush
        elapsedClock.tick(10L);
        aggregator.flush();

        // assert the state
        assertFalse(telemetryReporter.getMetrics().isEmpty());
        assertEquals(5, telemetryReporter.getMetrics().size());
        Map<String, MetricMeasurement> measurements =
            telemetryReporter.getMetrics().stream()
                .collect(Collectors.toMap(m -> m.getMetric().getName(), m -> m));

        assertMeasurement(measurements, 20L, "Foo.sum", 12);
        assertMeasurement(measurements, 20L, "Foo.avg", 4);
        assertMeasurement(measurements, 20L, "Foo.count", 3);
        assertMeasurement(measurements, 20L, "Foo.min", 1);
        assertMeasurement(measurements, 20L, "Foo.max", 9);
      }
    }
  }

  @Test
  void testReportMetricShouldProduceAggregationWithDifferentAttributes() {
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter telemetryReporter = new CollectingTelemetryReporter()) {
      try (TelemetryDatapointAggregator aggregator =
                   new TelemetryDatapointAggregator(telemetryReporter, elapsedClock)) {
        Metric metric1 = Metric.builder().name("Foo").attribute("X", "A").build();
        Metric metric2 = Metric.builder().name("Foo").attribute("X", "B").build();
        Metric metric3 = Metric.builder().name("Foo").attribute("X", "C").build();

        elapsedClock.tick(10L);
        // Produce metrics
        aggregator.reportComplete(
                MetricMeasurement.builder().metric(metric1).value(1).epochTimestampNanos(1).build());
        aggregator.reportComplete(
                MetricMeasurement.builder().metric(metric2).value(2).epochTimestampNanos(1).build());
        aggregator.reportComplete(
                MetricMeasurement.builder().metric(metric3).value(9).epochTimestampNanos(1).build());

        // Nothing should happen yet
        assertTrue(telemetryReporter.getMetrics().isEmpty());

        // Now flush
        elapsedClock.tick(10L);
        aggregator.flush();

        // assert the state
        assertFalse(telemetryReporter.getMetrics().isEmpty());
        assertEquals(5, telemetryReporter.getMetrics().size());
        Map<String, MetricMeasurement> measurements =
                telemetryReporter.getMetrics().stream()
                        .collect(Collectors.toMap(m -> m.getMetric().getName(), m -> m));

        assertMeasurement(measurements, 20L, "Foo.sum", 12);
        assertMeasurement(measurements, 20L, "Foo.avg", 4);
        assertMeasurement(measurements, 20L, "Foo.count", 3);
        assertMeasurement(measurements, 20L, "Foo.min", 1);
        assertMeasurement(measurements, 20L, "Foo.max", 9);
      }
    }
  }

  @Test
  void testReportMultipleMetrics() {
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter telemetryReporter = new CollectingTelemetryReporter()) {
      try (TelemetryDatapointAggregator aggregator =
                   new TelemetryDatapointAggregator(telemetryReporter, elapsedClock)) {
        Metric metric1 = Metric.builder().name("Foo").attribute("X", "Y").build();
        Metric metric2 = Metric.builder().name("Bar").attribute("X", "Y").build();

        elapsedClock.tick(10L);
        // Produce metrics
        aggregator.reportComplete(
                MetricMeasurement.builder().metric(metric1).value(1).epochTimestampNanos(1).build());
        aggregator.reportComplete(
                MetricMeasurement.builder().metric(metric1).value(2).epochTimestampNanos(1).build());
        aggregator.reportComplete(
                MetricMeasurement.builder().metric(metric1).value(9).epochTimestampNanos(1).build());

        // Produce metrics
        aggregator.reportComplete(
                MetricMeasurement.builder().metric(metric2).value(2).epochTimestampNanos(1).build());
        aggregator.reportComplete(
                MetricMeasurement.builder().metric(metric2).value(4).epochTimestampNanos(1).build());
        aggregator.reportComplete(
                MetricMeasurement.builder().metric(metric2).value(18).epochTimestampNanos(1).build());

        // Nothing should happen yet
        assertTrue(telemetryReporter.getMetrics().isEmpty());

        // Now flush
        elapsedClock.tick(10L);
        aggregator.flush();

        // assert the state
        assertFalse(telemetryReporter.getMetrics().isEmpty());
        assertEquals(10, telemetryReporter.getMetrics().size());
        Map<String, MetricMeasurement> measurements =
                telemetryReporter.getMetrics().stream()
                        .collect(Collectors.toMap(m -> m.getMetric().getName(), m -> m));

        assertMeasurement(measurements, 20L, "Foo.sum", 12);
        assertMeasurement(measurements, 20L, "Foo.avg", 4);
        assertMeasurement(measurements, 20L, "Foo.count", 3);
        assertMeasurement(measurements, 20L, "Foo.min", 1);
        assertMeasurement(measurements, 20L, "Foo.max", 9);

        assertMeasurement(measurements, 20L, "Bar.sum", 24);
        assertMeasurement(measurements, 20L, "Bar.avg", 8);
        assertMeasurement(measurements, 20L, "Bar.count", 3);
        assertMeasurement(measurements, 20L, "Bar.min", 2);
        assertMeasurement(measurements, 20L, "Bar.max", 18);

      }
    }
  }

  private static void assertMeasurement(
      Map<String, MetricMeasurement> measurements,
      long expectedEpochTimestampNanos,
      String expectedName,
      double expectedValue) {
    MetricMeasurement metricMeasurement = measurements.getOrDefault(expectedName, null);
    assertNotNull(metricMeasurement);
    assertNotNull(metricMeasurement.getMetric());
    assertNotNull(metricMeasurement.getKind());

    assertTrue(metricMeasurement.getMetric().getAttributes().isEmpty());
    assertEquals(MetricMeasurementKind.AGGREGATE, metricMeasurement.getKind());
    assertEquals(expectedName, metricMeasurement.getMetric().getName());
    assertEquals(expectedValue, metricMeasurement.getValue());
    assertEquals(expectedEpochTimestampNanos, metricMeasurement.getEpochTimestampNanos());
    assertEquals(MetricMeasurementKind.AGGREGATE, metricMeasurement.getKind());
  }
}
