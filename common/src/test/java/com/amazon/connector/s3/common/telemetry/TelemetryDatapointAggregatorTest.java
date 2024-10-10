/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.amazon.connector.s3.SpotBugsLambdaWorkaround;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class TelemetryDatapointAggregatorTest {
  @Test
  void testCreate() {
    TelemetryReporter telemetryReporter = mock(TelemetryReporter.class);
    Clock clock = mock(Clock.class);
    TelemetryDatapointAggregator aggregator =
        new TelemetryDatapointAggregator(telemetryReporter, Optional.empty(), clock);
    assertSame(telemetryReporter, aggregator.getTelemetryReporter());
    assertSame(clock, aggregator.getEpochClock());
  }

  @Test
  void testCreateWithNulls() {
    TelemetryReporter telemetryReporter = mock(TelemetryReporter.class);
    Clock clock = mock(Clock.class);
    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class,
        () -> new TelemetryDatapointAggregator(null, Optional.empty(), clock));
    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class,
        () -> new TelemetryDatapointAggregator(telemetryReporter, null));
  }

  @Test
  void testReportStartDoesNothing() {
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter telemetryReporter = new CollectingTelemetryReporter()) {
      try (TelemetryDatapointAggregator aggregator =
          new TelemetryDatapointAggregator(telemetryReporter, Optional.empty(), elapsedClock)) {
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
          new TelemetryDatapointAggregator(telemetryReporter, Optional.empty(), elapsedClock)) {
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
  void testReportOperationShouldProduceAggregation() {
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter telemetryReporter = new CollectingTelemetryReporter()) {
      try (TelemetryDatapointAggregator aggregator =
          new TelemetryDatapointAggregator(telemetryReporter, Optional.empty(), elapsedClock)) {
        Operation operation = Operation.builder().name("Foo").attribute("X", "Y").build();
        OperationMeasurement operationMeasurement1 =
            OperationMeasurement.builder()
                .operation(operation)
                .epochTimestampNanos(1L)
                .elapsedStartTimeNanos(1L)
                .elapsedCompleteTimeNanos(2L)
                .level(TelemetryLevel.CRITICAL)
                .build();

        OperationMeasurement operationMeasurement2 =
            OperationMeasurement.builder()
                .operation(operation)
                .epochTimestampNanos(1L)
                .elapsedStartTimeNanos(2L)
                .elapsedCompleteTimeNanos(4L)
                .level(TelemetryLevel.CRITICAL)
                .build();

        OperationMeasurement operationMeasurement3 =
            OperationMeasurement.builder()
                .operation(operation)
                .epochTimestampNanos(1L)
                .elapsedStartTimeNanos(3L)
                .elapsedCompleteTimeNanos(12L)
                .level(TelemetryLevel.CRITICAL)
                .build();

        elapsedClock.tick(10L);
        // Produce metrics
        aggregator.reportComplete(operationMeasurement1);
        aggregator.reportComplete(operationMeasurement2);
        aggregator.reportComplete(operationMeasurement3);

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
          new TelemetryDatapointAggregator(telemetryReporter, Optional.empty(), elapsedClock)) {
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
          new TelemetryDatapointAggregator(telemetryReporter, Optional.empty(), elapsedClock)) {
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

  @Test
  void testReportMultipleMetricsWithRegularFlush() throws InterruptedException {
    TickingClock elapsedClock = new TickingClock(0L);
    try (CollectingTelemetryReporter telemetryReporter = new CollectingTelemetryReporter()) {
      try (TelemetryDatapointAggregator aggregator =
          // FLush every 500 ms
          new TelemetryDatapointAggregator(
              telemetryReporter, Optional.of(Duration.of(500, ChronoUnit.MILLIS)), elapsedClock)) {
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

        // Now wait for 2 seconds. we will expect that this will be reported at least 3 times.
        // We check that we have 3 * [values per metric] measurements
        Thread.sleep(2000L);
        assertFalse(telemetryReporter.getMetrics().isEmpty());
        assertTrue(
            telemetryReporter.getMetrics().size()
                > TelemetryDatapointAggregator.AggregationKind.values().length * 3);
      }
      // Sleep for a second to avoid races with final flushing
      Thread.sleep(1000L);
      // At this point the aggregator it shut down.
      // Capture what has been reported and wait again
      // We should get nothing new
      int flushedMetrics = telemetryReporter.getMetrics().size();
      Thread.sleep(2000L);
      assertEquals(flushedMetrics, telemetryReporter.getMetrics().size());
    }
  }

  @SuppressFBWarnings(
      value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
      justification =
          "This is complaining about `executor.submit`. In this case we do not have any use for this Future")
  @Test
  void testConcurrentMetricsUpdates() throws InterruptedException {
    // We run 10 threads, each does 100 iterations
    // On each iteration, we are doing 10 ms waits, so each thread should run roughly 1 second
    // We set flush interval at 20ms, so around 50 flushes will happen then.
    // The idea here is to test concurrency of multiple writers and flush reader
    int threadCount = 10;
    int iterationCount = 100;
    long flushIntervalMillis = 20;
    long threadWaitTimeMillis = 10L;
    Metric metric1 = Metric.builder().name("Foo").attribute("X", "Y").build();
    Metric metric2 = Metric.builder().name("Bar").attribute("X", "Z").build();

    try (CollectingTelemetryReporter telemetryReporter = new CollectingTelemetryReporter()) {
      try (TelemetryDatapointAggregator aggregator =
          // FLush every 200 ms
          new TelemetryDatapointAggregator(
              telemetryReporter,
              Optional.of(Duration.of(flushIntervalMillis, ChronoUnit.MILLIS)),
              new TickingClock(0L))) {
        // Launch 10 threads, each report multiple metrics on the loop
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicLong sum1 = new AtomicLong();
        AtomicLong count1 = new AtomicLong();
        AtomicLong sum2 = new AtomicLong();
        AtomicLong count2 = new AtomicLong();

        try {
          for (int i = 0; i < threadCount; i++) {
            executor.submit(
                () -> {
                  for (int j = 0; j < iterationCount; j++) {
                    aggregator.reportComplete(
                        MetricMeasurement.builder()
                            .metric(metric1)
                            .value(j)
                            .epochTimestampNanos(j)
                            .build());
                    sum1.addAndGet(j);
                    count1.incrementAndGet();

                    aggregator.reportComplete(
                        MetricMeasurement.builder()
                            .metric(metric2)
                            .value(j + 1)
                            .epochTimestampNanos(j)
                            .build());

                    sum2.addAndGet(j + 1);
                    count2.incrementAndGet();

                    // Thank you, Java, for making me do this
                    try {
                      Thread.sleep(threadWaitTimeMillis);
                    } catch (InterruptedException e) {
                      throw new RuntimeException(e);
                    }
                  }
                });
          }
        } finally {
          // We wait forever, as we expect termination
          executor.shutdown();
          assertTrue(executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS));

          // It's not possible to know for sure how many times we flushed,
          // but we should have flushed something
          assertFalse(telemetryReporter.getMetrics().isEmpty());

          // Get our aggregates out of the aggregator and compare them with what we have
          TelemetryDatapointAggregator.Aggregation aggregation1 =
              aggregator.getAggregations().get(Metric.builder().name("Foo").build());
          TelemetryDatapointAggregator.Aggregation aggregation2 =
              aggregator.getAggregations().get(Metric.builder().name("Bar").build());

          assertNotNull(aggregation1);
          assertEquals(count1.get(), aggregation1.getCount());
          assertEquals(sum1.get(), aggregation1.getSum());
          assertEquals(0, aggregation1.getMin());
          assertEquals(iterationCount - 1, aggregation1.getMax());

          assertNotNull(aggregation2);
          assertEquals(count2.get(), aggregation2.getCount());
          assertEquals(sum2.get(), aggregation2.getSum());
          assertEquals(1, aggregation2.getMin());
          assertEquals(iterationCount, aggregation2.getMax());
        }
        System.out.println(telemetryReporter.getMetrics().size());
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
