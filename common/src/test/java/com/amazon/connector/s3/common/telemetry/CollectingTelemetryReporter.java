package com.amazon.connector.s3.common.telemetry;

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
