package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

public class TelemetryTest {
  @Test
  void testCreateTelemetry() {
    Telemetry newTelemetry = Telemetry.getTelemetry(TelemetryConfiguration.DEFAULT);
    assertInstanceOf(ConfigurableTelemetry.class, newTelemetry);
    ConfigurableTelemetry telemetry = (ConfigurableTelemetry) newTelemetry;

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
  void testCreateTelemetryWithNulls() {
    assertThrows(NullPointerException.class, () -> Telemetry.getTelemetry(null));
  }

  @Test
  void testNoOp() {
    Telemetry noopTelemetry = Telemetry.NOOP;
    assertInstanceOf(DefaultTelemetry.class, noopTelemetry);
    DefaultTelemetry telemetry = (DefaultTelemetry) noopTelemetry;
    assertInstanceOf(NoOpTelemetryReporter.class, telemetry.getReporter());
  }
}
