package com.amazon.connector.s3;

import com.amazon.connector.s3.common.ConnectorConfiguration;
import com.amazon.connector.s3.common.telemetry.TelemetryConfiguration;
import com.amazon.connector.s3.request.ObjectClient;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

/** Configuration for {@link ObjectClient} */
@Getter
@Builder
@EqualsAndHashCode
public class ObjectClientConfiguration {
  public static final String DEFAULT_USER_AGENT_PREFIX = null;
  public static final String USER_AGENT_PREFIX_KEY = "useragentprefix";
  public static final String TELEMETRY_PREFIX = "telemetry";

  /** User Agent Prefix. {@link ObjectClientConfiguration#DEFAULT_USER_AGENT_PREFIX} by default. */
  @Builder.Default private String userAgentPrefix = DEFAULT_USER_AGENT_PREFIX;

  /** Telemetry configuration */
  @Builder.Default @NonNull private TelemetryConfiguration telemetryConfiguration = TelemetryConfiguration.DEFAULT;

  public static final ObjectClientConfiguration DEFAULT =
      ObjectClientConfiguration.builder().build();

  /**
   * Constructs {@link ObjectClientConfiguration} from {@link ConnectorConfiguration} object.
   *
   * @param configuration Configuration object to generate ObjectClientConfiguration from
   * @return ObjectClientConfiguration
   */
  public static ObjectClientConfiguration fromConfiguration(ConnectorConfiguration configuration) {
    return ObjectClientConfiguration.builder()
        .userAgentPrefix(configuration.getString(USER_AGENT_PREFIX_KEY, DEFAULT_USER_AGENT_PREFIX))
        .telemetryConfiguration(
            TelemetryConfiguration.fromConfiguration(configuration.map(TELEMETRY_PREFIX)))
        .build();
  }
}
