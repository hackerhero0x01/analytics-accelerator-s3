  /*
   * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   *
   * Licensed under the Apache License, Version 2.0 (the "License").
   * You may not use this file except in compliance with the License.
   * You may obtain a copy of the License at
   *
   *   http://www.apache.org/licenses/LICENSE-2.0
  
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
package com.amazon.connector.s3.common.telemetry;

import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import org.slf4j.event.Level;

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
    this(configuration, createTelemetryReporter(configuration));
  }

  /**
   * Creates a new instance of {@link ConfigurableTelemetry}.
   *
   * @param configuration an instance of {@link TelemetryConfiguration} that configures this
   *     telemetry.
   * @param telemetryReporter an instance of {@link TelemetryReporter}
   */
  private ConfigurableTelemetry(
      TelemetryConfiguration configuration, TelemetryReporter telemetryReporter) {
    super(
        DefaultEpochClock.DEFAULT,
        DefaultElapsedClock.DEFAULT,
        telemetryReporter,
        createTelemetryAggregator(configuration, telemetryReporter),
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
                  Level.valueOf(configuration.getLoggingLevel()),
                  EpochFormatter.DEFAULT));
    }

    // Create console reporter.
    if (configuration.isStdOutEnabled()) {
      stdOutTelemetryReporter =
          Optional.of(new PrintStreamTelemetryReporter(System.out, EpochFormatter.DEFAULT));
    }

    // Create the final reporter
    if (stdOutTelemetryReporter.isPresent() && loggingReporter.isPresent()) {
      // if both reporters are present, create a group
      return new GroupTelemetryReporter(
          Arrays.asList(stdOutTelemetryReporter.get(), loggingReporter.get()));
    } else if (loggingReporter.isPresent()) {
      // if only logging reporter is present, this is all there is
      return loggingReporter.get();
    } else if (stdOutTelemetryReporter.isPresent()) {
      // if only console reporter is present, this is all there is
      return stdOutTelemetryReporter.get();
    } else {
      // all reporters disabled. resort to NoOp
      return new NoOpTelemetryReporter();
    }
  }

  /**
   * Creates {@link TelemetryDatapointAggregator}, if configured
   *
   * @param configuration {@link ConfigurableTelemetry} configuration.
   * @param telemetryReporter an instance of {@link TelemetryReporter}
   * @return {@link TelemetryDatapointAggregator}, if configured
   */
  private static Optional<TelemetryDatapointAggregator> createTelemetryAggregator(
      TelemetryConfiguration configuration, TelemetryReporter telemetryReporter) {
    // If aggregations are enabled, wrap the resulting reporter
    if (configuration.isAggregationsEnabled()) {
      TelemetryDatapointAggregator telemetryDatapointAggregator =
          new TelemetryDatapointAggregator(
              telemetryReporter, configuration.getAggregationsFlushInterval());
      return Optional.of(telemetryDatapointAggregator);
    } else {
      return Optional.empty();
    }
  }
}
