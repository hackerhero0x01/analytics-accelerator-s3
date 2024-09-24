package com.amazon.connector.s3.common.telemetry;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** This represents the metric measurement kind */
@Getter
@AllArgsConstructor
public enum MetricMeasurementKind {
  RAW(1), // raw data point, reported directly to telemetry
  AGGREGATE(2); // aggregate value, reported as a result of aggregation
  private final int value;
}
