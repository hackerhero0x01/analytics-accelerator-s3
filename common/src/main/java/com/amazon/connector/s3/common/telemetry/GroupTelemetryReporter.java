package com.amazon.connector.s3.common.telemetry;

import java.util.Collection;
import java.util.Collections;
import lombok.Getter;
import lombok.NonNull;

/** A {@link TelemetryReporter that reports telemetry to a group of independent reporterts} */
@Getter
class GroupTelemetryReporter implements TelemetryReporter {
  private final @NonNull Collection<TelemetryReporter> reporters;

  /**
   * Creates a new instance of {@link GroupTelemetryReporter}.
   *
   * @param reporters a group of reporters to fan out telemetry events to.
   */
  public GroupTelemetryReporter(@NonNull Collection<TelemetryReporter> reporters) {
    this.reporters = Collections.unmodifiableCollection(reporters);
  }

  /**
   * Reports the start of an operation
   *
   * @param epochTimestampNanos wall clock time for the operation start
   * @param operation and instance of {@link Operation} to start
   */
  @Override
  public void reportStart(long epochTimestampNanos, Operation operation) {
    for (TelemetryReporter reporter : reporters) {
      reporter.reportStart(epochTimestampNanos, operation);
    }
  }

  /**
   * Outputs the current contents of {@link OperationMeasurement} to each of the supplied reporters.
   *
   * @param operationMeasurement operation execution.
   */
  @Override
  public void reportComplete(OperationMeasurement operationMeasurement) {
    for (TelemetryReporter reporter : reporters) {
      reporter.reportComplete(operationMeasurement);
    }
  }
}
