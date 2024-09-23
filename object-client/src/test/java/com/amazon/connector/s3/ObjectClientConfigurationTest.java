package com.amazon.connector.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.amazon.connector.s3.common.ConnectorConfiguration;
import com.amazon.connector.s3.common.telemetry.TelemetryConfiguration;
import com.amazon.connector.s3.common.telemetry.TelemetryLevel;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class ObjectClientConfigurationTest {
  private static final String CONFIGURATION_PREFIX = "s3.connector";
  private static final String PROPERTY_PREFIX = "objectclient";

  @Test
  void testDefaultBuilder() {
    ObjectClientConfiguration configuration = ObjectClientConfiguration.builder().build();
    assertEquals(ObjectClientConfiguration.DEFAULT, configuration);
  }

  @Test
  void testNulls() {
    assertThrows(
        NullPointerException.class,
        () -> ObjectClientConfiguration.builder().telemetryConfiguration(null));
  }

  @Test
  void testNonDefaults() {
    TelemetryConfiguration telemetryConfiguration =
        TelemetryConfiguration.builder().level(TelemetryLevel.VERBOSE.toString()).build();
    ObjectClientConfiguration configuration =
        ObjectClientConfiguration.builder()
            .userAgentPrefix("newUserAgent")
            .telemetryConfiguration(telemetryConfiguration)
            .build();
    assertEquals("newUserAgent", configuration.getUserAgentPrefix());
    assertEquals(
        TelemetryLevel.VERBOSE.toString(), configuration.getTelemetryConfiguration().getLevel());
  }

  @Test
  void testFromConfiguration() {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        "s3.connector.objectclient." + ObjectClientConfiguration.USER_AGENT_PREFIX_KEY,
        "newUserAgent");
    properties.put(
        "s3.connector.objectclient."
            + ObjectClientConfiguration.TELEMETRY_PREFIX
            + "."
            + TelemetryConfiguration.LEVEL_KEY,
        TelemetryLevel.VERBOSE.toString());

    ConnectorConfiguration configuration =
        new ConnectorConfiguration(properties, CONFIGURATION_PREFIX);
    configuration = configuration.map(PROPERTY_PREFIX);
    ObjectClientConfiguration objectClientConfiguration =
        ObjectClientConfiguration.fromConfiguration(configuration);
    assertEquals("newUserAgent", objectClientConfiguration.getUserAgentPrefix());
    assertEquals(
        TelemetryLevel.VERBOSE.toString(),
        objectClientConfiguration.getTelemetryConfiguration().getLevel());
  }
}
