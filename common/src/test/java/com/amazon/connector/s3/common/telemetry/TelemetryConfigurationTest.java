package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import com.amazon.connector.s3.common.ConnectorConfiguration;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class TelemetryConfigurationTest {
  private static final String TEST_PREFIX = "s3.connector";

  @Test
  void testDefault() {
    TelemetryConfiguration configuration = TelemetryConfiguration.builder().build();
    assertEquals(TelemetryConfiguration.DEFAULT, configuration);
  }

  @Test
  void testDefaultValues() {
    TelemetryConfiguration configuration = TelemetryConfiguration.DEFAULT;
    assertFalse(configuration.isStdOutEnabled());
    assertTrue(configuration.isLoggingEnabled());
    assertEquals(Level.INFO.toString(), configuration.getLoggingLevel());
    assertEquals(TelemetryConfiguration.DEFAULT_LOGGING_NAME, configuration.getLoggingName());
    assertEquals(Optional.empty(), configuration.getAggregationsFlushInterval());
  }

  @Test
  void testDefaultFromConfiguration() {
    TelemetryConfiguration configuration =
        TelemetryConfiguration.fromConfiguration(
            new ConnectorConfiguration(new HashMap<>(), TEST_PREFIX));
    assertEquals(TelemetryConfiguration.DEFAULT, configuration);
    assertFalse(configuration.isStdOutEnabled());
    assertTrue(configuration.isLoggingEnabled());
    assertFalse(configuration.isAggregationsEnabled());
    assertEquals(Optional.empty(), configuration.getAggregationsFlushInterval());
  }

  @Test
  void testNonDefaultFromConfiguration() {
    TelemetryConfiguration configuration =
        TelemetryConfiguration.fromConfiguration(getConfiguration());
    assertFalse(configuration.isLoggingEnabled());
    assertFalse(configuration.isStdOutEnabled());
    assertEquals("debug", configuration.getLoggingLevel());
    assertEquals("foo", configuration.getLoggingName());
    assertTrue(configuration.isAggregationsEnabled());
    assertEquals(
        Optional.of(Duration.of(30, ChronoUnit.SECONDS)),
        configuration.getAggregationsFlushInterval());
  }

  @Test
  void testNulls() {
    assertThrows(
        NullPointerException.class,
        () -> TelemetryConfiguration.builder().loggingName(null).build());
    assertThrows(
        NullPointerException.class,
        () -> TelemetryConfiguration.builder().loggingLevel(null).build());
    assertThrows(
        NullPointerException.class,
        () -> TelemetryConfiguration.builder().aggregationsFlushInterval(null).build());
    assertThrows(NullPointerException.class, () -> TelemetryConfiguration.fromConfiguration(null));
  }

  /**
   * Creates non default configuration
   *
   * @return a new configuration.
   */
  public static ConnectorConfiguration getConfiguration() {
    Map<String, String> properties = new HashMap<>();
    properties.put(TEST_PREFIX + "." + TelemetryConfiguration.LOGGING_ENABLED_KEY, "false");
    properties.put(TEST_PREFIX + "." + TelemetryConfiguration.STD_OUT_ENABLED_KEY, "false");
    properties.put(TEST_PREFIX + "." + TelemetryConfiguration.LOGGING_NAME_KEY, "foo");
    properties.put(TEST_PREFIX + "." + TelemetryConfiguration.LOGGING_LEVEL_KEY, "debug");
    properties.put(TEST_PREFIX + "." + TelemetryConfiguration.AGGREGATIONS_ENABLED_KEY, "true");
    properties.put(
        TEST_PREFIX + "." + TelemetryConfiguration.AGGREGATIONS_FLUSH_INTERVAL_SECONDS_KEY, "30");

    return new ConnectorConfiguration(properties, TEST_PREFIX);
  }
}
