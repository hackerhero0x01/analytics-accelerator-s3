package com.amazon.connector.s3.common.telemetry;

import java.util.ArrayList;
import java.util.Collection;
import lombok.Getter;

/** This reporter simply collects the {@link OperationMeasurement} objects. */
@Getter
public class CollectingTelemetryReporter implements TelemetryReporter {
  /** RAll seen operation executions. */
  private final Collection<OperationMeasurement> operationMeasurements = new ArrayList<>();

  /**
   * Reports this {@link OperationMeasurement}.
   *
   * @param operationMeasurement - operation execution.
   */
  @Override
  public void report(OperationMeasurement operationMeasurement) {
    this.operationMeasurements.add(operationMeasurement);
  }
}
