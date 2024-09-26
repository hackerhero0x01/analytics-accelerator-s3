package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

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

      assertInstanceOf(GroupTelemetryReporter.class, telemetry.getReporter());
      GroupTelemetryReporter groupTelemetryReporterTopLevel =
          (GroupTelemetryReporter) telemetry.getReporter();
      assertEquals(2, groupTelemetryReporterTopLevel.getReporters().size());
      TelemetryReporter[] telemetryReportersTopLevel = new TelemetryReporter[2];
      groupTelemetryReporterTopLevel.getReporters().toArray(telemetryReportersTopLevel);

      assertInstanceOf(TelemetryDatapointAggregator.class, telemetryReportersTopLevel[1]);
      TelemetryDatapointAggregator telemetryDatapointAggregator =
          (TelemetryDatapointAggregator) telemetryReportersTopLevel[1];
      assertSame(
          telemetryDatapointAggregator.getTelemetryReporter(), telemetryReportersTopLevel[0]);

      assertInstanceOf(GroupTelemetryReporter.class, telemetryReportersTopLevel[0]);
      GroupTelemetryReporter groupTelemetryReporter =
          (GroupTelemetryReporter) telemetryReportersTopLevel[0];
      assertEquals(2, groupTelemetryReporter.getReporters().size());
      TelemetryReporter[] telemetryReporters = new TelemetryReporter[2];
      groupTelemetryReporter.getReporters().toArray(telemetryReporters);

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
}
