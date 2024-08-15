package com.amazon.connector.s3.common.telemetry;

/**
 * Default clock used in all non-test contexts to measure wall clock time. Uses `{@link
 * System#currentTimeMillis()}.
 */
final class DefaultEpochClock implements Clock {
  /**
   * Returns current timestamp in nanoseconds.
   *
   * @return current timestamp in nanoseconds.
   */
  @Override
  public long getCurrentTimeNanos() {
    return System.currentTimeMillis() * 1_000_000;
  }

  /** Default clock. */
  public static final Clock DEFAULT = new DefaultEpochClock();
}
