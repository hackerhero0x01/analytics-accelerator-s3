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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

public class ConfigurableTelemetryTest {
  @Test
  void testCreateAllEnabledWithoutAggregation() {
    TelemetryConfiguration configuration =
        TelemetryConfiguration.builder().loggingEnabled(true).stdOutEnabled(true).build();
    try (ConfigurableTelemetry telemetry = new ConfigurableTelemetry(configuration)) {

      // verify correct clocks
      assertSame(DefaultEpochClock.DEFAULT, telemetry.getEpochClock());
      assertSame(DefaultElapsedClock.DEFAULT, telemetry.getElapsedClock());

      assertInstanceOf(GroupTelemetryReporter.class, telemetry.getReporter());
      GroupTelemetryReporter groupTelemetryReporter =
          (GroupTelemetryReporter) telemetry.getReporter();
      assertEquals(2, groupTelemetryReporter.getReporters().size());
      TelemetryReporter[] telemetryReporters = new TelemetryReporter[2];
      groupTelemetryReporter.getReporters().toArray(telemetryReporters);

      assertInstanceOf(PrintStreamTelemetryReporter.class, telemetryReporters[0]);
      PrintStreamTelemetryReporter printStreamTelemetryReporter =
          (PrintStreamTelemetryReporter) telemetryReporters[0];
      assertSame(printStreamTelemetryReporter.getEpochFormatter(), EpochFormatter.DEFAULT);
      assertSame(printStreamTelemetryReporter.getPrintStream(), System.out);

      assertInstanceOf(LoggingTelemetryReporter.class, telemetryReporters[1]);
      LoggingTelemetryReporter loggingTelemetryReporter =
          (LoggingTelemetryReporter) telemetryReporters[1];
      assertSame(loggingTelemetryReporter.getEpochFormatter(), EpochFormatter.DEFAULT);
      assertEquals(loggingTelemetryReporter.getLoggerLevel(), Level.INFO);
      assertEquals(
          loggingTelemetryReporter.getLoggerName(), LoggingTelemetryReporter.DEFAULT_LOGGING_NAME);
    }
  }

  @Test
  void testCreateAllEnabledWithAggregation() {
    TelemetryConfiguration configuration =
        TelemetryConfiguration.builder()
            .loggingEnabled(true)
            .stdOutEnabled(true)
            .aggregationsEnabled(true)
            .aggregationsFlushInterval(Optional.of(Duration.of(40, ChronoUnit.SECONDS)))
            .build();
    try (ConfigurableTelemetry telemetry = new ConfigurableTelemetry(configuration)) {
      // verify correct clocks
      assertSame(DefaultEpochClock.DEFAULT, telemetry.getEpochClock());
      assertSame(DefaultElapsedClock.DEFAULT, telemetry.getElapsedClock());

      // verify all the reporters
      assertInstanceOf(GroupTelemetryReporter.class, telemetry.getReporter());
      GroupTelemetryReporter groupTelemetryReporter =
          (GroupTelemetryReporter) telemetry.getReporter();
      assertEquals(2, groupTelemetryReporter.getReporters().size());
      TelemetryReporter[] telemetryReporters = new TelemetryReporter[2];
      groupTelemetryReporter.getReporters().toArray(telemetryReporters);

      assertInstanceOf(PrintStreamTelemetryReporter.class, telemetryReporters[0]);
      PrintStreamTelemetryReporter printStreamTelemetryReporter =
          (PrintStreamTelemetryReporter) telemetryReporters[0];
      assertSame(printStreamTelemetryReporter.getEpochFormatter(), EpochFormatter.DEFAULT);
      assertSame(printStreamTelemetryReporter.getPrintStream(), System.out);

      assertInstanceOf(LoggingTelemetryReporter.class, telemetryReporters[1]);
      LoggingTelemetryReporter loggingTelemetryReporter =
          (LoggingTelemetryReporter) telemetryReporters[1];
      assertSame(loggingTelemetryReporter.getEpochFormatter(), EpochFormatter.DEFAULT);
      assertEquals(loggingTelemetryReporter.getLoggerLevel(), Level.INFO);
      assertEquals(
          loggingTelemetryReporter.getLoggerName(), LoggingTelemetryReporter.DEFAULT_LOGGING_NAME);

      // verify the aggregator
      assertNotNull(telemetry.getAggregator());
      assertNotNull(telemetry.getAggregator().get());
      TelemetryDatapointAggregator telemetryDatapointAggregator = telemetry.getAggregator().get();
      assertEquals(DefaultEpochClock.DEFAULT, telemetryDatapointAggregator.getEpochClock());
      assertEquals(telemetry.getReporter(), telemetryDatapointAggregator.getTelemetryReporter());
    }
  }

  @Test
  void testCreateCustomizeParameters() {
    TelemetryConfiguration configuration =
        TelemetryConfiguration.builder()
            .loggingEnabled(true)
            .stdOutEnabled(true)
            .loggingName("foo")
            .loggingLevel(Level.DEBUG.toString())
            .build();

    ConfigurableTelemetry telemetry = new ConfigurableTelemetry(configuration);

    // verify correct clocks
    assertSame(DefaultEpochClock.DEFAULT, telemetry.getEpochClock());
    assertSame(DefaultElapsedClock.DEFAULT, telemetry.getElapsedClock());

    assertInstanceOf(GroupTelemetryReporter.class, telemetry.getReporter());
    GroupTelemetryReporter groupTelemetryReporter =
        (GroupTelemetryReporter) telemetry.getReporter();
    assertEquals(2, groupTelemetryReporter.getReporters().size());
    TelemetryReporter[] telemetryReporters = new TelemetryReporter[2];
    groupTelemetryReporter.getReporters().toArray(telemetryReporters);

    assertInstanceOf(PrintStreamTelemetryReporter.class, telemetryReporters[0]);
    PrintStreamTelemetryReporter printStreamTelemetryReporter =
        (PrintStreamTelemetryReporter) telemetryReporters[0];
    assertSame(printStreamTelemetryReporter.getEpochFormatter(), EpochFormatter.DEFAULT);
    assertSame(printStreamTelemetryReporter.getPrintStream(), System.out);

    assertInstanceOf(LoggingTelemetryReporter.class, telemetryReporters[1]);
    LoggingTelemetryReporter loggingTelemetryReporter =
        (LoggingTelemetryReporter) telemetryReporters[1];
    assertSame(loggingTelemetryReporter.getEpochFormatter(), EpochFormatter.DEFAULT);
    assertEquals(loggingTelemetryReporter.getLoggerLevel(), Level.DEBUG);
    assertEquals(loggingTelemetryReporter.getLoggerName(), "foo");
  }

  @Test
  void testCreateConsoleOnly() {
    TelemetryConfiguration configuration =
        TelemetryConfiguration.builder().loggingEnabled(false).stdOutEnabled(true).build();

    ConfigurableTelemetry telemetry = new ConfigurableTelemetry(configuration);

    // verify correct clocks
    assertSame(DefaultEpochClock.DEFAULT, telemetry.getEpochClock());
    assertSame(DefaultElapsedClock.DEFAULT, telemetry.getElapsedClock());

    assertInstanceOf(PrintStreamTelemetryReporter.class, telemetry.getReporter());
    PrintStreamTelemetryReporter printStreamTelemetryReporter =
        (PrintStreamTelemetryReporter) telemetry.getReporter();
    assertSame(printStreamTelemetryReporter.getEpochFormatter(), EpochFormatter.DEFAULT);
    assertSame(printStreamTelemetryReporter.getPrintStream(), System.out);
  }

  @Test
  void testCreateLoggingOnly() {
    TelemetryConfiguration configuration =
        TelemetryConfiguration.builder()
            .loggingName("foo")
            .loggingLevel(Level.DEBUG.toString())
            .stdOutEnabled(false)
            .loggingEnabled(true)
            .build();

    ConfigurableTelemetry telemetry = new ConfigurableTelemetry(configuration);

    // verify correct clocks
    assertSame(DefaultEpochClock.DEFAULT, telemetry.getEpochClock());
    assertSame(DefaultElapsedClock.DEFAULT, telemetry.getElapsedClock());

    assertInstanceOf(LoggingTelemetryReporter.class, telemetry.getReporter());
    LoggingTelemetryReporter loggingTelemetryReporter =
        (LoggingTelemetryReporter) telemetry.getReporter();

    assertSame(loggingTelemetryReporter.getEpochFormatter(), EpochFormatter.DEFAULT);
    assertEquals(loggingTelemetryReporter.getLoggerLevel(), Level.DEBUG);
    assertEquals(loggingTelemetryReporter.getLoggerName(), "foo");
  }

  @Test
  void testCreateNeither() {
    TelemetryConfiguration configuration =
        TelemetryConfiguration.builder().loggingEnabled(false).stdOutEnabled(false).build();

    ConfigurableTelemetry telemetry = new ConfigurableTelemetry(configuration);

    // verify correct clocks
    assertSame(DefaultEpochClock.DEFAULT, telemetry.getEpochClock());
    assertSame(DefaultElapsedClock.DEFAULT, telemetry.getElapsedClock());

    assertInstanceOf(NoOpTelemetryReporter.class, telemetry.getReporter());
  }

  @Test
  void testCreateWithDefaultTelemetryFormat() {
    TelemetryConfiguration configuration =
        TelemetryConfiguration.builder().stdOutEnabled(true).loggingEnabled(false).build();

    ConfigurableTelemetry telemetry = new ConfigurableTelemetry(configuration);
    PrintStreamTelemetryReporter reporter = (PrintStreamTelemetryReporter) telemetry.getReporter();

    assertInstanceOf(DefaultTelemetryFormat.class, reporter.getTelemetryFormat());
  }

  @Test
  void testCreateWithJSONTelemetryFormat() {
    TelemetryConfiguration configuration =
        TelemetryConfiguration.builder()
            .stdOutEnabled(true)
            .loggingEnabled(false)
            .telemetryFormat("json")
            .build();

    ConfigurableTelemetry telemetry = new ConfigurableTelemetry(configuration);
    PrintStreamTelemetryReporter reporter = (PrintStreamTelemetryReporter) telemetry.getReporter();

    assertInstanceOf(JSONTelemetryFormat.class, reporter.getTelemetryFormat());
  }

  @Test
  void testCreateWithUnknownTelemetryFormat() {
    TelemetryConfiguration configuration =
        TelemetryConfiguration.builder()
            .stdOutEnabled(true)
            .loggingEnabled(false)
            .telemetryFormat("nonsense")
            .build();

    Exception e =
        assertThrows(
            IllegalArgumentException.class, () -> new ConfigurableTelemetry(configuration));
    assertTrue(e.getMessage().contains("Unsupported telemetry format: nonsense"), e.getMessage());
  }
}
