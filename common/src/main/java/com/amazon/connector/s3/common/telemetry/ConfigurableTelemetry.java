package com.amazon.connector.s3.common.telemetry;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import org.apache.logging.log4j.Level;

/**
 * This is a version of {@link DefaultTelemetry} that is assembled from the configuration. This is a
 * convenience class, that makes it easier to plug this into the rest of the stack. This is the only
 * public telemetry class exposed outside of this package.
 */
public class ConfigurableTelemetry extends DefaultTelemetry {
  /**
   * Creates a new instance of {@link ConfigurableTelemetry}.
   *
   * @param configuration an instance of {@link TelemetryConfiguration} that configures this
   *     telemetry.
   */
  public ConfigurableTelemetry(TelemetryConfiguration configuration) {
    super(
        DefaultEpochClock.DEFAULT,
        DefaultElapsedClock.DEFAULT,
        createTelemetryReporter(configuration),
        TelemetryLevel.valueOf(configuration.getLevel().toUpperCase(Locale.ROOT)));
  }

  /**
   * Creates the telemetry reporter based on the configuration
   *
   * @param configuration {@link ConfigurableTelemetry} configuration.
   * @return a new instance of {@link ConfigurableTelemetry}.
   */
  private static TelemetryReporter createTelemetryReporter(TelemetryConfiguration configuration) {
    Optional<LoggingTelemetryReporter> loggingReporter = Optional.empty();
    Optional<PrintStreamTelemetryReporter> stdOutTelemetryReporter = Optional.empty();
    // Create logging reporter
    if (configuration.isLoggingEnabled()) {
      loggingReporter =
          Optional.of(
              new LoggingTelemetryReporter(
                  configuration.getLoggingName(),
                  Level.getLevel(configuration.getLoggingLevel()),
                  EpochFormatter.DEFAULT));
    }

    // Create console reporter.
    if (configuration.isStdOutEnabled()) {
      stdOutTelemetryReporter =
          Optional.of(new PrintStreamTelemetryReporter(System.out, EpochFormatter.DEFAULT));
    }

    // Create the final reporter
    TelemetryReporter telemetryReporter;
    if (stdOutTelemetryReporter.isPresent() && loggingReporter.isPresent()) {
      // if both reporters are present, create a group
      telemetryReporter =
          new GroupTelemetryReporter(
              Arrays.asList(stdOutTelemetryReporter.get(), loggingReporter.get()));
    } else if (loggingReporter.isPresent()) {
      // if only logging reporter is present, this is all there is
      telemetryReporter = loggingReporter.get();
    } else if (stdOutTelemetryReporter.isPresent()) {
      // if only console reporter is present, this is all there is
      telemetryReporter = stdOutTelemetryReporter.get();
    } else {
      // all reporters disabled. resort to NoOp
      telemetryReporter = new NoOpTelemetryReporter();
    }

    // If aggregations are enabled, wrap the resulting reporter
    if (configuration.isAggregationsEnabled()) {
      TelemetryDatapointAggregator telemetryDatapointAggregator =
          new TelemetryDatapointAggregator(
              telemetryReporter, configuration.getAggregationsFlushInterval());
      return new GroupTelemetryReporter(
          Arrays.asList(telemetryReporter, telemetryDatapointAggregator));
    } else {
      return telemetryReporter;
    }
  }
}
