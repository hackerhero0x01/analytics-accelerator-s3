package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

public class ConfigurableTelemetryTest {
  @Test
  void testCreateDefault() {
    TelemetryConfiguration configuration = TelemetryConfiguration.DEFAULT;
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
    assertEquals(loggingTelemetryReporter.getLoggerLevel(), Level.INFO);
    assertEquals(
        loggingTelemetryReporter.getLoggerName(), LoggingTelemetryReporter.DEFAULT_LOGGER_NAME);
  }

  @Test
  void testCreateCustomizeParameters() {
    TelemetryConfiguration configuration =
        TelemetryConfiguration.builder().loggerName("foo").logLevel(Level.DEBUG.toString()).build();

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
        TelemetryConfiguration.builder().enableLogging(false).build();

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
            .loggerName("foo")
            .logLevel(Level.DEBUG.toString())
            .enableStdOut(false)
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
        TelemetryConfiguration.builder().enableLogging(false).enableStdOut(false).build();

    ConfigurableTelemetry telemetry = new ConfigurableTelemetry(configuration);

    // verify correct clocks
    assertSame(DefaultEpochClock.DEFAULT, telemetry.getEpochClock());
    assertSame(DefaultElapsedClock.DEFAULT, telemetry.getElapsedClock());

    assertInstanceOf(NoOpTelemetryReporter.class, telemetry.getReporter());
  }
}
