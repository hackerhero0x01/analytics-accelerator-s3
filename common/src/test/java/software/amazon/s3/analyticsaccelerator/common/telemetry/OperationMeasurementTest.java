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
package software.amazon.s3.analyticsaccelerator.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class OperationMeasurementTest {

  @Test
  void testCreateOperationMeasurement() {
    Operation operation = Operation.builder().name("foo").build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .level(TelemetryLevel.STANDARD)
            .epochTimestampNanos(1)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(12)
            .build();

    assertSame(operation, operationMeasurement.getOperation());
    assertSame(operation, operationMeasurement.getDatapoint());
    assertEquals(1, operationMeasurement.getEpochTimestampNanos());
    assertEquals(10, operationMeasurement.getElapsedStartTimeNanos());
    assertEquals(12, operationMeasurement.getElapsedCompleteTimeNanos());
    assertEquals(2, operationMeasurement.getElapsedTimeNanos());
    assertEquals(2, operationMeasurement.getValue());

    assertFalse(operationMeasurement.getError().isPresent());
    assertTrue(operationMeasurement.succeeded());
    assertFalse(operationMeasurement.failed());
  }

  @Test
  void testCreateOperationMeasurementWithError() {
    Operation operation = Operation.builder().name("foo").build();
    Throwable error = new IllegalStateException("Error");
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .level(TelemetryLevel.STANDARD)
            .epochTimestampNanos(1)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(12)
            .error(error)
            .build();

    assertSame(operation, operationMeasurement.getOperation());

    assertEquals(1, operationMeasurement.getEpochTimestampNanos());
    assertEquals(10, operationMeasurement.getElapsedStartTimeNanos());
    assertEquals(12, operationMeasurement.getElapsedCompleteTimeNanos());
    assertEquals(2, operationMeasurement.getElapsedTimeNanos());

    assertTrue(operationMeasurement.getError().isPresent());
    assertSame(error, operationMeasurement.getError().get());
    assertFalse(operationMeasurement.succeeded());
    assertTrue(operationMeasurement.failed());
  }

  @Test
  void testCreateOperationMeasurementWithNulls() {
    assertThrows(
        NullPointerException.class,
        () -> {
          OperationMeasurement.builder()
              .epochTimestampNanos(1)
              .elapsedStartTimeNanos(10)
              .elapsedCompleteTimeNanos(12)
              .build();
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          OperationMeasurement.builder()
              .epochTimestampNanos(1)
              .elapsedStartTimeNanos(10)
              .elapsedCompleteTimeNanos(12)
              .operation(null)
              .build();
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          OperationMeasurement.builder()
              .epochTimestampNanos(1)
              .operation(Operation.builder().name("foo").build())
              .level(null)
              .elapsedStartTimeNanos(10)
              .elapsedCompleteTimeNanos(12)
              .build();
        });
  }

  @Test
  void testCreateOperationMeasurementWithInvalidTimestampNanos() {
    Operation operation = Operation.builder().name("foo").build();
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          OperationMeasurement.builder()
              .operation(operation)
              .elapsedStartTimeNanos(10)
              .elapsedCompleteTimeNanos(12)
              .build();
        });
  }

  @Test
  void testCreateOperationMeasurementWithInvalidStartTimeNanos() {
    Operation operation = Operation.builder().name("foo").build();
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          OperationMeasurement.builder()
              .operation(operation)
              .epochTimestampNanos(1)
              .elapsedCompleteTimeNanos(12)
              .build();
        });
  }

  @Test
  void testCreateOperationMeasurementWithInvalidCompleteTimeNanos() {
    Operation operation = Operation.builder().name("foo").build();
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          OperationMeasurement.builder()
              .operation(operation)
              .epochTimestampNanos(1)
              .elapsedStartTimeNanos(10)
              .build();
        });
  }

  @Test
  void testCreateOperationMeasurementWithStartAfterComplete() {
    Operation operation = Operation.builder().name("foo").build();
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          OperationMeasurement.builder()
              .operation(operation)
              .epochTimestampNanos(1)
              .elapsedStartTimeNanos(12)
              .elapsedCompleteTimeNanos(10)
              .build();
        });
  }
}
