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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.Getter;

/** This reporter simply collects the {@link OperationMeasurement} objects. */
@Getter
public class CollectingTelemetryReporter implements TelemetryReporter {
  /** All seen operation executions. */
  private final Collection<TelemetryDatapointMeasurement> datapointCompletions = new ArrayList<>();

  private final Collection<Operation> operationStarts = new ArrayList<>();
  private final AtomicBoolean flushed = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** Clears state */
  public synchronized void clear() {
    this.datapointCompletions.clear();
    this.operationStarts.clear();
  }

  /**
   * Reports the start of an operation
   *
   * @param epochTimestampNanos wall clock time for the operation start
   * @param operation and instance of {@link Operation} to start
   */
  @Override
  public synchronized void reportStart(long epochTimestampNanos, Operation operation) {
    this.operationStarts.add(operation);
  }

  /**
   * Reports this {@link OperationMeasurement}.
   *
   * @param datapointMeasurement - operation execution.
   */
  @Override
  public synchronized void reportComplete(TelemetryDatapointMeasurement datapointMeasurement) {
    this.datapointCompletions.add(datapointMeasurement);
  }

  /**
   * Returns dataPoints that correspond to operation completions
   *
   * @return dataPoints that correspond to operation completions
   */
  public synchronized Collection<OperationMeasurement> getOperationCompletions() {
    return this.getDatapointCompletions().stream()
        .filter(OperationMeasurement.class::isInstance)
        .map(OperationMeasurement.class::cast)
        .collect(Collectors.toCollection(ArrayList::new));
  }

  /**
   * Returns dataPoints that correspond to operation completions
   *
   * @return dataPoints that correspond to operation completions
   */
  public synchronized Collection<MetricMeasurement> getMetrics() {
    return this.getDatapointCompletions().stream()
        .filter(MetricMeasurement.class::isInstance)
        .map(MetricMeasurement.class::cast)
        .collect(Collectors.toCollection(ArrayList::new));
  }

  /** Flushes any intermediate state of the reporters */
  @Override
  public synchronized void flush() {
    this.flushed.set(true);
  }

  /** Closes the reporter */
  @Override
  public synchronized void close() {
    this.closed.set(true);
    TelemetryReporter.super.close();
  }
}
