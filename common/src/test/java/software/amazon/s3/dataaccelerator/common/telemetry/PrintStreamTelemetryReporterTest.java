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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.PrintStream;
import java.time.ZoneId;
import java.util.Locale;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.s3.dataaccelerator.SpotBugsLambdaWorkaround;

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
