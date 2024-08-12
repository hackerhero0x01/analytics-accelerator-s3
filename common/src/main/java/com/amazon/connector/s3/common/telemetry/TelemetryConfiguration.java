package com.amazon.connector.s3.common.telemetry;

import java.util.logging.Level;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Configuration for {@link ConfigurableTelemetry}. The options here are on the scrappy side - not
 * everything that can be supported is supported. These can be added later if needed (e.g. {@link
 * EpochFormatter#getPattern()} and similar.
 */
@Value
@Builder
public class TelemetryConfiguration {
  /** Enable standard output. */
  @Builder.Default boolean enableStdOut = true;
  /** Enable logging output. */
  @Builder.Default boolean enableLogging = true;
  /** Logging level. */
  @Builder.Default @NonNull String logLevel = Level.INFO.toString();
  /** Logger name. */
  @Builder.Default @NonNull String loggerName = LoggingTelemetryReporter.DEFAULT_LOGGER_NAME;

  /** Default configuration for {@link ConfigurableTelemetry}. */
  public static TelemetryConfiguration DEFAULT = TelemetryConfiguration.builder().build();
}
