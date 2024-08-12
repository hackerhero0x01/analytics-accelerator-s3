package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class TelemetryConfigurationTest {
  @Test
  void testDefault() {
    TelemetryConfiguration configuration = TelemetryConfiguration.builder().build();
    assertEquals(TelemetryConfiguration.DEFAULT, configuration);
  }

  @Test
  void testNulls() {
    assertThrows(
        NullPointerException.class,
        () -> TelemetryConfiguration.builder().loggerName(null).build());
    assertThrows(
        NullPointerException.class, () -> TelemetryConfiguration.builder().logLevel(null).build());
  }
}
