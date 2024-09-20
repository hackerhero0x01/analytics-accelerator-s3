package com.amazon.connector.s3;

import com.amazon.connector.s3.common.telemetry.*;

public final class TestTelemetry {
  private TestTelemetry() {}

  /**
   * Default telemetry to use for tests - combination of NoOp telemetry with VERBOSE level provides
   * the best code coverage
   */
  public static final Telemetry DEFAULT =
      Telemetry.createTelemetry(
          TelemetryConfiguration.builder()
              .loggingEnabled(false)
              .stdOutEnabled(false)
              .level(TelemetryLevel.VERBOSE.toString())
              .build());
}
