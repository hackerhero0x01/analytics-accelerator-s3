package com.amazon.connector.s3.common.telemetry;

/**
 * A function that mimics {@link java.util.function.Supplier}, but allows exceptions to be thrown
 *
 * @param <T>
 */
@FunctionalInterface
public interface TelemetrySupplier<T> {
  /**
   * Functional representation of the code that takes no parameters and returns a value of type
   * {@link T}. The code is allowed to throw any exception.
   *
   * @return a value of type {@link T}.
   * @throws Throwable on error condition.
   */
  T apply() throws Throwable;
}
