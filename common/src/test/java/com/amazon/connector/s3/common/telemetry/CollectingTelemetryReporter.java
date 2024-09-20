package com.amazon.connector.s3.common.telemetry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;

/** This reporter simply collects the {@link OperationMeasurement} objects. */
@Getter
public class CollectingTelemetryReporter implements TelemetryReporter {
  /** All seen operation executions. */
  private final Collection<OperationMeasurement> operationCompletions = new ArrayList<>();

  private final Collection<Operation> operationStarts = new ArrayList<>();
  private final AtomicBoolean flushed = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** Clears state */
  public void clear() {
    this.operationCompletions.clear();
    this.operationStarts.clear();
  }

  /**
   * Reports the start of an operation
   *
   * @param epochTimestampNanos wall clock time for the operation start
   * @param operation and instance of {@link Operation} to start
   */
  @Override
  public void reportStart(long epochTimestampNanos, Operation operation) {
    this.operationStarts.add(operation);
  }

  /**
   * Reports this {@link OperationMeasurement}.
   *
   * @param operationMeasurement - operation execution.
   */
  @Override
  public void reportComplete(OperationMeasurement operationMeasurement) {
    this.operationCompletions.add(operationMeasurement);
  }

  /** Flushes any intermediate state of the reporters */
  @Override
  public void flush() {
    this.flushed.set(true);
  }

  /** Closes the reporter */
  @Override
  public void close() {
    this.closed.set(true);
    TelemetryReporter.super.close();
  }
}
