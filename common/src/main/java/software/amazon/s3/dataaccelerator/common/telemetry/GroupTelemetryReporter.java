/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.dataaccelerator.common.telemetry;

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
   * @param datapointMeasurement operation execution.
   */
  @Override
  public void reportComplete(TelemetryDatapointMeasurement datapointMeasurement) {
    for (TelemetryReporter reporter : reporters) {
      reporter.reportComplete(datapointMeasurement);
    }
  }

  /** Flushes any intermediate state of the reporters */
  @Override
  public void flush() {
    for (TelemetryReporter reporter : reporters) {
      reporter.flush();
    }
  }

  /** Closes the underlying reporters */
  @Override
  public void close() {
    for (TelemetryReporter reporter : reporters) {
      reporter.close();
    }
  }
}
