package com.amazon.connector.s3.common.telemetry;

import com.amazon.connector.s3.common.ConnectorConfiguration;
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
  public static final String STD_OUT_ENABLED_KEY = "std.out.enabled";
  public static final boolean DEFAULT_STD_OUT_ENABLED = true;

  public static final String LOGGING_ENABLED_KEY = "logging.enabled";
  public static final boolean DEFAULT_LOGGING_ENABLED = true;

  public static final String LOGGING_LEVEL_KEY = "logging.level";
  public static final String DEFAULT_LOGGING_LEVEL = Level.INFO.toString();

  public static final String LOGGING_NAME_KEY = "logging.name";
  public static final String DEFAULT_LOGGING_NAME = LoggingTelemetryReporter.DEFAULT_LOGGING_NAME;

  /** Enable standard output. */
  @Builder.Default boolean stdOutEnabled = DEFAULT_STD_OUT_ENABLED;
  /** Enable logging output. */
  @Builder.Default boolean loggingEnabled = DEFAULT_LOGGING_ENABLED;
  /** Logging level. */
  @Builder.Default @NonNull String loggingLevel = DEFAULT_LOGGING_LEVEL;
  /** Logger name. */
  @Builder.Default @NonNull String loggingName = DEFAULT_LOGGING_NAME;

  /** Default configuration for {@link ConfigurableTelemetry}. */
  public static TelemetryConfiguration DEFAULT = TelemetryConfiguration.builder().build();

  /**
   * Constructs {@link TelemetryConfiguration} from {@link ConnectorConfiguration} object.
   *
   * @param configuration Configuration object to generate PhysicalIOConfiguration from
   * @return LogicalIOConfiguration
   */
  public static TelemetryConfiguration fromConfiguration(
      @NonNull ConnectorConfiguration configuration) {
    return TelemetryConfiguration.builder()
        .stdOutEnabled(configuration.getBoolean(STD_OUT_ENABLED_KEY, DEFAULT_STD_OUT_ENABLED))
        .loggingEnabled(configuration.getBoolean(LOGGING_ENABLED_KEY, DEFAULT_LOGGING_ENABLED))
        .loggingName(configuration.getString(LOGGING_NAME_KEY, DEFAULT_LOGGING_NAME))
        .loggingLevel(configuration.getString(LOGGING_LEVEL_KEY, DEFAULT_LOGGING_LEVEL))
        .build();
  }
}
