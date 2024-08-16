package com.amazon.connector.s3.common.telemetry;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** This defines the level of reporting for telemetry operations */
@Getter
@AllArgsConstructor
public enum TelemetryLevel {
  CRITICAL("CRITICAL"),
  STANDARD("STANDARD"),
  VERBOSE("VERBOSE");
  private final String name;

  /**
   * Get numeric priority of telemetry level.
   *
   * @return numeric priority of telemetry level.
   */
  public int getPriority() {
    switch (this) {
      case CRITICAL:
        return 2;
      case STANDARD:
        return 1;
      case VERBOSE:
        return 0;
      default:
        throw new IllegalArgumentException("Unknown telemetry level: " + this);
    }
  }
}
