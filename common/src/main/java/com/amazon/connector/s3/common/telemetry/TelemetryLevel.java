package com.amazon.connector.s3.common.telemetry;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** This defines the level of reporting for telemetry operations */
@Getter
@AllArgsConstructor
public enum TelemetryLevel {
  CRITICAL(2),
  STANDARD(1),
  VERBOSE(0);
  private final int value;
}
