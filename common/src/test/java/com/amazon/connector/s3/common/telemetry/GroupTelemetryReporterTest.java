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
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class GroupTelemetryReporterTest {
  private static final long TEST_EPOCH_NANOS = 1722944779101123456L;

  @Test
  void testCreate() {
    CollectingTelemetryReporter reporter1 = new CollectingTelemetryReporter();
    CollectingTelemetryReporter reporter2 = new CollectingTelemetryReporter();
    List<TelemetryReporter> reporters = new ArrayList<>();
    reporters.add(reporter1);
    reporters.add(reporter2);
    try (GroupTelemetryReporter reporter = new GroupTelemetryReporter(reporters)) {
      assertArrayEquals(reporter.getReporters().toArray(), reporters.toArray());
    }
  }

  @Test
  void testFlushedAndClosed() {
    CollectingTelemetryReporter reporter1 = new CollectingTelemetryReporter();
    CollectingTelemetryReporter reporter2 = new CollectingTelemetryReporter();
    List<TelemetryReporter> reporters = new ArrayList<>();
    reporters.add(reporter1);
    reporters.add(reporter2);
    try (GroupTelemetryReporter reporter = new GroupTelemetryReporter(reporters)) {
      reporter.flush();
      assertTrue(reporter1.getFlushed().get());
      assertTrue(reporter2.getFlushed().get());
      assertFalse(reporter1.getClosed().get());
      assertFalse(reporter2.getClosed().get());
    }
    assertTrue(reporter1.getClosed().get());
    assertTrue(reporter2.getClosed().get());
  }

  @Test
  void testCreateWithNulls() {
    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class, () -> new GroupTelemetryReporter(null));
  }

  @Test
  void testReportComplete() {
    CollectingTelemetryReporter reporter1 = new CollectingTelemetryReporter();
    CollectingTelemetryReporter reporter2 = new CollectingTelemetryReporter();
    List<TelemetryReporter> reporters = new ArrayList<>();
    reporters.add(reporter1);
    reporters.add(reporter2);
    try (GroupTelemetryReporter reporter = new GroupTelemetryReporter(reporters)) {
      Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
      OperationMeasurement operationMeasurement =
          OperationMeasurement.builder()
              .operation(operation)
              .epochTimestampNanos(TEST_EPOCH_NANOS)
              .level(TelemetryLevel.STANDARD)
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
  }

  @Test
  void testReportStart() {
    CollectingTelemetryReporter reporter1 = new CollectingTelemetryReporter();
    CollectingTelemetryReporter reporter2 = new CollectingTelemetryReporter();
    List<TelemetryReporter> reporters = new ArrayList<>();
    reporters.add(reporter1);
    reporters.add(reporter2);
    try (GroupTelemetryReporter reporter = new GroupTelemetryReporter(reporters)) {
      Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
      reporter.reportStart(TEST_EPOCH_NANOS, operation);

      reporters.forEach(
          r ->
              assertArrayEquals(
                  ((CollectingTelemetryReporter) r).getOperationStarts().toArray(),
                  new Operation[] {operation}));
    }
  }
}
