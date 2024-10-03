package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.amazon.connector.s3.SpotBugsLambdaWorkaround;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.PrintStream;
import java.time.ZoneId;
import java.util.Locale;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class PrintStreamTelemetryReporterTest {
  private static final long TEST_EPOCH_NANOS = 1722944779101123456L;

  @Test
  public void testCreate() {
    try (PrintStreamTelemetryReporter reporter = new PrintStreamTelemetryReporter(System.out)) {
      assertEquals(reporter.getPrintStream(), System.out);
      assertEquals(reporter.getEpochFormatter(), EpochFormatter.DEFAULT);
    }
  }

  @Test
  public void testCreateWithEpochFormatter() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            "yyyy/MM/dd'T'HH;mm;ss,SSS'Z'",
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    try (PrintStreamTelemetryReporter reporter =
        new PrintStreamTelemetryReporter(System.out, epochFormatter)) {
      assertEquals(reporter.getPrintStream(), System.out);
      assertEquals(reporter.getEpochFormatter(), epochFormatter);
    }
  }

  @Test
  public void testCreateWithNulls() {
    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class, () -> new PrintStreamTelemetryReporter(null));
    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class, () -> new PrintStreamTelemetryReporter(System.out, null));
    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class,
        () -> new PrintStreamTelemetryReporter(null, EpochFormatter.DEFAULT));
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

    PrintStream printStream = mock(PrintStream.class);
    try (PrintStreamTelemetryReporter reporter = new PrintStreamTelemetryReporter(printStream)) {
      reporter.reportComplete(operationMeasurement);
      ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);

      verify(printStream).println(stringCaptor.capture());
      String threadAttributeAsString =
          CommonAttributes.THREAD_ID.getName() + "=" + Thread.currentThread().getId();
      assertTrue(
          stringCaptor
              .getValue()
              .contains("foo(A=42, " + threadAttributeAsString + "): 4,999,990 ns"));
    }
  }

  @Test
  public void testFlush() {
    PrintStream printStream = mock(PrintStream.class);
    try (PrintStreamTelemetryReporter reporter = new PrintStreamTelemetryReporter(printStream)) {
      reporter.flush();
      verify(printStream).flush();
    }
  }

  @Test
  public void testReportCompleteEpochFormatter() {
    Operation operation = Operation.builder().id("123").name("foo").attribute("A", 42).build();
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
            "yyyy/MM/dd'T'HH;mm;ss,SSS'Z'",
            TimeZone.getTimeZone(ZoneId.of("GMT", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);

    PrintStream printStream = mock(PrintStream.class);
    try (PrintStreamTelemetryReporter reporter =
        new PrintStreamTelemetryReporter(printStream, epochFormatter)) {
      reporter.reportComplete(operationMeasurement);
      ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);

      verify(printStream).println(stringCaptor.capture());
      String threadAttributeAsString =
          CommonAttributes.THREAD_ID.getName() + "=" + Thread.currentThread().getId();
      assertEquals(
          "[2024/08/06T11;46;19,101Z] [success] [123] foo(A=42, "
              + threadAttributeAsString
              + "): 4,999,990 ns",
          stringCaptor.getValue());
    }
  }

  @Test
  public void testReportCompleteThrowsOnNull() {
    assertThrows(
        NullPointerException.class,
        () -> {
          try (PrintStreamTelemetryReporter reporter =
              new PrintStreamTelemetryReporter(System.out)) {
            reporter.reportComplete(null);
          }
        });
  }

  @Test
  public void testReportStart() {
    Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
    PrintStream printStream = mock(PrintStream.class);
    try (PrintStreamTelemetryReporter reporter = new PrintStreamTelemetryReporter(printStream)) {
      reporter.reportStart(TEST_EPOCH_NANOS, operation);
      ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);

      verify(printStream).println(stringCaptor.capture());
      String threadAttributeAsString =
          CommonAttributes.THREAD_ID.getName() + "=" + Thread.currentThread().getId();
      assertTrue(stringCaptor.getValue().contains("foo(A=42, " + threadAttributeAsString + ")"));
      assertTrue(stringCaptor.getValue().contains("[  start]"));
    }
  }

  @Test
  public void testReportStartEpochFormatter() {
    Operation operation = Operation.builder().id("123").name("foo").attribute("A", 42).build();
    EpochFormatter epochFormatter =
        new EpochFormatter(
            "yyyy/MM/dd'T'HH;mm;ss,SSS'Z'",
            TimeZone.getTimeZone(ZoneId.of("GMT", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);

    PrintStream printStream = mock(PrintStream.class);
    PrintStreamTelemetryReporter reporter =
        new PrintStreamTelemetryReporter(printStream, epochFormatter);
    reporter.reportStart(TEST_EPOCH_NANOS, operation);
    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);

    verify(printStream).println(stringCaptor.capture());
    String threadAttributeAsString =
        CommonAttributes.THREAD_ID.getName() + "=" + Thread.currentThread().getId();
    assertEquals(
        "[2024/08/06T11;46;19,101Z] [  start] [123] foo(A=42, " + threadAttributeAsString + ")",
        stringCaptor.getValue());
  }

  @Test
  public void testReportStartThrowsOnNull() {
    assertThrows(
        NullPointerException.class,
        () -> new PrintStreamTelemetryReporter(System.out).reportStart(TEST_EPOCH_NANOS, null));
  }
}
