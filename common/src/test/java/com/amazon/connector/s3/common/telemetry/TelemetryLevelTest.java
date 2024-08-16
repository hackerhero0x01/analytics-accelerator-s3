package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class TelemetryLevelTest {
  @Test
  void testGetPriority() {
    assertEquals(0, TelemetryLevel.VERBOSE.getPriority());
    assertEquals(1, TelemetryLevel.STANDARD.getPriority());
    assertEquals(2, TelemetryLevel.CRITICAL.getPriority());
  }
}
