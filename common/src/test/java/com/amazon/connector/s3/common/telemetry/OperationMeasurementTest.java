package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import java.time.ZoneId;
import java.util.Locale;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

public class OperationMeasurementTest {
  private static final long TEST_EPOCH_NANOS = 1722944779101123456L;

  @Test
  void testCreateOperationMeasurement() {
    Operation operation = Operation.builder().name("foo").build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .epochTimestampNanos(1)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(12)
            .build();

    assertSame(operation, operationMeasurement.getOperation());

    assertEquals(1, operationMeasurement.getEpochTimestampNanos());
    assertEquals(10, operationMeasurement.getElapsedStartTimeNanos());
    assertEquals(12, operationMeasurement.getElapsedCompleteTimeNanos());
    assertEquals(2, operationMeasurement.getElapsedTimeNanos());

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

  @Test
  void testToString() {
    Operation operation = Operation.builder().id("123").name("foo").build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    String toString = operationMeasurement.toString();
    assertTrue(toString.contains(" [success] [123] foo(thread_id=1): 4,999,990 ns"));
  }

  @Test
  void testFullToString() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            EpochFormatter.DEFAULT_PATTERN,
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    Operation operation = Operation.builder().id("123").name("foo").build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    String toString = operationMeasurement.toString(epochFormatter);
    assertEquals(
        toString,
        "[2024-08-06T17:46:19.101Z] [success] [123] foo(thread_id="
            + +Thread.currentThread().getId()
            + "): 4,999,990 ns");
  }

  @Test
  void testFullToStringWithError() {
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
            .error(error)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    String toString = operationMeasurement.toString(epochFormatter);
    assertEquals(
        toString,
        "[2024-08-06T17:46:19.101Z] [failure] [123] foo(thread_id="
            + Thread.currentThread().getId()
            + "): 4,999,990 ns [java.lang.IllegalStateException: 'Error']");
  }

  @Test
  void testToStringWithFormatter() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            "yyyy/MM/dd'T'HH;mm;ss,SSS'Z'",
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);

    Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    String toString = operationMeasurement.toString(epochFormatter);
    String threadAttributeAsString =
        CommonAttributes.THREAD_ID.getName() + "=" + Thread.currentThread().getId();
    assertTrue(toString.contains("foo(A=42, " + threadAttributeAsString + "): 4,999,990 ns"));
  }

  @Test
  void testToStringWithFormatterAndFormatString() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            "yyyy/MM/dd'T'HH;mm;ss,SSS'Z'",
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);

    Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();
    String threadAttributeAsString =
        CommonAttributes.THREAD_ID.getName() + "=" + Thread.currentThread().getId();

    String toString = operationMeasurement.toString(epochFormatter, "<%s> <%s> <%s> <%d>");
    assertTrue(toString.contains("foo(A=42, " + threadAttributeAsString + ")> <4999990>"));
  }

  @Test
  void testToStringWithNulls() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            "yyyy/MM/dd'T'HH;mm;ss,SSS'Z'",
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);

    Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    assertThrows(NullPointerException.class, () -> operationMeasurement.toString(null));
    assertThrows(NullPointerException.class, () -> operationMeasurement.toString(null, "foo"));
    assertThrows(
        NullPointerException.class, () -> operationMeasurement.toString(epochFormatter, null));
  }

  @Test
  void testGetOperationStartingString() {
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
        OperationMeasurement.getOperationStartingString(
            operation, TEST_EPOCH_NANOS, epochFormatter, "[%s] [  start] %s"));

    assertEquals(
        "[2024-08-06T17:46:19.101Z] [  start] [123] foo(thread_id="
            + Thread.currentThread().getId()
            + ")",
        OperationMeasurement.getOperationStartingString(
            operation, TEST_EPOCH_NANOS, epochFormatter));
    assertTrue(
        OperationMeasurement.getOperationStartingString(operation, TEST_EPOCH_NANOS)
            .contains("[  start] [123] foo(thread_id=" + Thread.currentThread().getId() + ")"));
  }

  @Test
  void testGetOperationStartingStringNulls() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            EpochFormatter.DEFAULT_PATTERN,
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    Operation operation = Operation.builder().id("123").name("foo").build();

    assertThrows(
        NullPointerException.class,
        () ->
            OperationMeasurement.getOperationStartingString(
                null, TEST_EPOCH_NANOS, epochFormatter, "%s, %s"));
    assertThrows(
        NullPointerException.class,
        () ->
            OperationMeasurement.getOperationStartingString(
                operation, TEST_EPOCH_NANOS, null, "%s, %s"));
    assertThrows(
        NullPointerException.class,
        () ->
            OperationMeasurement.getOperationStartingString(
                operation, TEST_EPOCH_NANOS, epochFormatter, null));
  }
}
