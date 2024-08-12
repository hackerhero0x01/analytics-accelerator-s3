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
   * Outputs the current contents of {@link OperationMeasurement} to each of the supplied reporters.
   *
   * @param operationMeasurement operation execution.
   */
  @Override
  public void report(OperationMeasurement operationMeasurement) {
    for (TelemetryReporter reporter : reporters) {
      reporter.report(operationMeasurement);
    }
  }
}
