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
package software.amazon.s3.dataaccelerator.common.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.ZoneId;
import java.util.Locale;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

public class DefaultTelemetryFormatTest {

  private static final TelemetryFormat telemetryFormat = new DefaultTelemetryFormat();

  private static final long TEST_EPOCH_NANOS = 1722944779101123456L;

  @Test
  void testRenderOperationStartWithNull() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            EpochFormatter.DEFAULT_PATTERN,
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    Operation operation = Operation.builder().id("123").name("foo").build();

    assertThrows(
        NullPointerException.class,
        () -> telemetryFormat.renderOperationStart(null, TEST_EPOCH_NANOS, epochFormatter));
    assertThrows(
        NullPointerException.class,
        () -> telemetryFormat.renderOperationStart(operation, TEST_EPOCH_NANOS, null));
  }

  @Test
  void testRenderOperationStart() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            EpochFormatter.DEFAULT_PATTERN,
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    Operation operation = Operation.builder().id("123").name("foo").build();

    assertEquals(
        "[2024-08-06T17:46:19.101Z] [  start] [123] foo(thread_id="
            + Thread.currentThread().getId()
            + ")",
        telemetryFormat.renderOperationStart(operation, TEST_EPOCH_NANOS, epochFormatter));

    assertEquals(
        "[2024-08-06T17:46:19.101Z] [  start] [123] foo(thread_id="
            + Thread.currentThread().getId()
            + ")",
        telemetryFormat.renderOperationStart(operation, TEST_EPOCH_NANOS, epochFormatter));
  }

  @Test
  void testRenderOperationEndWithNull() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            EpochFormatter.DEFAULT_PATTERN,
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    Operation operation = Operation.builder().id("123").name("foo").build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .level(TelemetryLevel.STANDARD)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    assertThrows(
        NullPointerException.class, () -> telemetryFormat.renderOperationEnd(null, epochFormatter));
    assertThrows(
        NullPointerException.class,
        () -> telemetryFormat.renderOperationEnd(operationMeasurement, null));
  }

  @Test
  void testRenderOperationEnd() {
    Operation operation = Operation.builder().id("123").name("foo").build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .level(TelemetryLevel.STANDARD)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();
    EpochFormatter epochFormatter =
        new EpochFormatter(
            EpochFormatter.DEFAULT_PATTERN,
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);

    String toString = telemetryFormat.renderOperationEnd(operationMeasurement, epochFormatter);
    assertTrue(toString.contains(" [success] [123] foo(thread_id=1): 4,999,990 ns"));
  }

  @Test
  void testRenderDatapointMeasurementWithNull() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            EpochFormatter.DEFAULT_PATTERN,
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    Operation operation = Operation.builder().id("123").name("foo").build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .level(TelemetryLevel.STANDARD)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    assertThrows(
        NullPointerException.class,
        () -> telemetryFormat.renderDatapointMeasurement(null, epochFormatter));
    assertThrows(
        NullPointerException.class,
        () -> telemetryFormat.renderDatapointMeasurement(operationMeasurement, null));
  }

  @Test
  void testRenderDatapointMeasurement() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            EpochFormatter.DEFAULT_PATTERN,
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    Operation operation = Operation.builder().id("123").name("foo").build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .level(TelemetryLevel.STANDARD)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    String toString =
        telemetryFormat.renderDatapointMeasurement(operationMeasurement, epochFormatter);
    assertEquals(
        toString,
        "[2024-08-06T17:46:19.101Z] [success] [123] foo(thread_id="
            + +Thread.currentThread().getId()
            + "): 4,999,990 ns");
  }

  @Test
  void testRenderDatapointMeasurementWithError() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            EpochFormatter.DEFAULT_PATTERN,
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    Operation operation = Operation.builder().id("123").name("foo").build();
    Exception error = new IllegalStateException("Error");
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .level(TelemetryLevel.STANDARD)
            .error(error)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    String toString =
        telemetryFormat.renderDatapointMeasurement(operationMeasurement, epochFormatter);
    assertEquals(
        toString,
        "[2024-08-06T17:46:19.101Z] [failure] [123] foo(thread_id="
            + Thread.currentThread().getId()
            + "): 4,999,990 ns [java.lang.IllegalStateException: 'Error']");
  }

  @Test
  void testRenderMetricMeasurementWithNull() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            EpochFormatter.DEFAULT_PATTERN,
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    Metric metric = Metric.builder().name("testMetric").build();
    MetricMeasurement metricMeasurement =
        MetricMeasurement.builder()
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .metric(metric)
            .value(1.0)
            .kind(MetricMeasurementKind.RAW)
            .build();

    assertThrows(
        NullPointerException.class,
        () -> telemetryFormat.renderMetricMeasurement(null, epochFormatter));
    assertThrows(
        NullPointerException.class,
        () -> telemetryFormat.renderMetricMeasurement(metricMeasurement, null));
  }

  @Test
  void testRenderMetricMeasurement() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            EpochFormatter.DEFAULT_PATTERN,
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    Metric metric = Metric.builder().name("testMetric").build();
    MetricMeasurement metricMeasurement =
        MetricMeasurement.builder()
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .metric(metric)
            .value(1.0)
            .kind(MetricMeasurementKind.RAW)
            .build();

    String toString = telemetryFormat.renderMetricMeasurement(metricMeasurement, epochFormatter);
    assertEquals(toString, "[2024-08-06T17:46:19.101Z] testMetric: 1.00");
  }
}
