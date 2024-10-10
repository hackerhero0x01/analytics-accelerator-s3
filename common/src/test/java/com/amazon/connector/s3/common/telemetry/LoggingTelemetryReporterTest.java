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

import com.amazon.connector.s3.SpotBugsLambdaWorkaround;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.ZoneId;
import java.util.Locale;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class LoggingTelemetryReporterTest {
  private static final long TEST_EPOCH_NANOS = 1722944779101123456L;

  @Test
  void testCreate() {
    try (LoggingTelemetryReporter reporter = new LoggingTelemetryReporter()) {
      assertEquals(LoggingTelemetryReporter.DEFAULT_LOGGING_NAME, reporter.getLoggerName());
      assertEquals(LoggingTelemetryReporter.DEFAULT_LOGGING_LEVEL, reporter.getLoggerLevel());
      assertEquals(EpochFormatter.DEFAULT, reporter.getEpochFormatter());

      Operation operation = Operation.builder().id("123").name("foo").attribute("A", 42).build();
      OperationMeasurement operationMeasurement =
          OperationMeasurement.builder()
              .operation(operation)
              .level(TelemetryLevel.STANDARD)
              .epochTimestampNanos(TEST_EPOCH_NANOS)
              .elapsedStartTimeNanos(10)
              .elapsedCompleteTimeNanos(5000000)
              .build();

      reporter.reportStart(TEST_EPOCH_NANOS, operation);
      reporter.reportComplete(operationMeasurement);
    }
  }

  @Test
  void testCreateWithArguments() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            "yyyy/MM/dd'T'HH;mm;ss,SSS'Z'",
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    try (LoggingTelemetryReporter reporter =
        new LoggingTelemetryReporter("foo", Level.ERROR, epochFormatter)) {
      assertEquals("foo", reporter.getLoggerName());
      assertEquals(Level.ERROR, reporter.getLoggerLevel());
      assertEquals(epochFormatter, reporter.getEpochFormatter());
    }
  }

  @Test
  public void testReportComplete() {
    Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .level(TelemetryLevel.STANDARD)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    try (LoggingTelemetryReporter reporter = new LoggingTelemetryReporter()) {
      reporter.reportComplete(operationMeasurement);
    }
  }

  @Test
  public void testReportDataPointComplete() {
    Metric metric = Metric.builder().name("S3.GET").attribute("Foo", "Bar").build();
    MetricMeasurement metricMeasurement =
        MetricMeasurement.builder()
            .metric(metric)
            .epochTimestampNanos(42L)
            .value(123L)
            .kind(MetricMeasurementKind.AGGREGATE)
            .build();

    try (LoggingTelemetryReporter reporter = new LoggingTelemetryReporter()) {
      reporter.reportComplete(metricMeasurement);
    }
  }

  @Test
  public void testReportCompleteWithException() {
    Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .level(TelemetryLevel.STANDARD)
            .error(new IllegalStateException("Error"))
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    try (LoggingTelemetryReporter reporter = new LoggingTelemetryReporter()) {
      reporter.reportComplete(operationMeasurement);
    }
  }

  @Test
  void testCreateWithNulls() {
    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class,
        () -> new LoggingTelemetryReporter(null, Level.ERROR, EpochFormatter.DEFAULT));
    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class,
        () -> new LoggingTelemetryReporter("foo", null, EpochFormatter.DEFAULT));
    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class, () -> new LoggingTelemetryReporter("foo", Level.ERROR, null));
  }

  @Test
  public void testReportCompleteThrowsOnNull() {
    assertThrows(
        NullPointerException.class,
        () -> {
          try (LoggingTelemetryReporter reporter = new LoggingTelemetryReporter()) {
            reporter.reportComplete(null);
          }
        });
  }
}
