package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.PrintStream;
import java.time.ZoneId;
import java.util.Locale;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class PrintStreamTelemetryReporterTest {
  @Test
  public void testCreate() {
    PrintStreamTelemetryReporter reporter = new PrintStreamTelemetryReporter(System.out);
    assertEquals(reporter.getPrintStream(), System.out);
    assertEquals(reporter.getEpochFormatter(), EpochFormatter.DEFAULT);
  }

  @Test
  public void testCreateWithEpochFormatter() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            "yyyy/MM/dd'T'HH;mm;ss,SSS'Z'",
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    PrintStreamTelemetryReporter reporter =
        new PrintStreamTelemetryReporter(System.out, epochFormatter);
    assertEquals(reporter.getPrintStream(), System.out);
    assertEquals(reporter.getEpochFormatter(), epochFormatter);
  }

  @Test
  public void testCreateWithNulls() {
    assertThrows(NullPointerException.class, () -> new PrintStreamTelemetryReporter(null));
    assertThrows(
        NullPointerException.class, () -> new PrintStreamTelemetryReporter(System.out, null));
    assertThrows(
        NullPointerException.class,
        () -> new PrintStreamTelemetryReporter(null, EpochFormatter.DEFAULT));
  }

  @Test
  public void testReport() {
    Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .epochTimestampNanos(1722944779101123456L)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    PrintStream printStream = mock(PrintStream.class);
    PrintStreamTelemetryReporter reporter = new PrintStreamTelemetryReporter(printStream);
    reporter.report(operationMeasurement);
    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);

    verify(printStream).println(stringCaptor.capture());
    String threadAttributeAsString =
        CommonAttributes.THREAD_ID.getName() + "=" + Thread.currentThread().getId();
    assertTrue(
        stringCaptor
            .getValue()
            .contains("foo(A=42, " + threadAttributeAsString + "): 4,999,990 ns"));
  }

  @Test
  public void testReportEpochFormatter() {
    Operation operation = Operation.builder().id("123").name("foo").attribute("A", 42).build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .epochTimestampNanos(1722944779101123456L)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    EpochFormatter epochFormatter =
        new EpochFormatter(
            "yyyy/MM/dd'T'HH;mm;ss,SSS'Z'",
            TimeZone.getTimeZone(ZoneId.of("GMT", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);

    PrintStream printStream = mock(PrintStream.class);
    PrintStreamTelemetryReporter reporter =
        new PrintStreamTelemetryReporter(printStream, epochFormatter);
    reporter.report(operationMeasurement);
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

  @Test
  public void testReportThrowsOnNull() {
    assertThrows(
        NullPointerException.class,
        () -> new PrintStreamTelemetryReporter(System.out).report(null));
  }
}
