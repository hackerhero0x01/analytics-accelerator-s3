package com.amazon.connector.s3.common.telemetry;

/** A functional interface that mimics {@link Runnable}, but allows exceptions to be thrown. */
@FunctionalInterface
public interface TelemetryAction {
  /**
   * Functional representation of the code that takes no parameters and returns no value. The code
   * is allowed to throw any exception.
   *
   * @throws Exception on error condition.
   */
  void apply() throws Throwable;
}
