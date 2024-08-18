package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class TelemetryLevelTest {
  @Test
  void testGetPriority() {
    assertEquals(0, TelemetryLevel.VERBOSE.getValue());
    assertEquals(1, TelemetryLevel.STANDARD.getValue());
    assertEquals(2, TelemetryLevel.CRITICAL.getValue());
  }
}
