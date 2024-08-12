package com.amazon.connector.s3.common.telemetry;

/**
 * Default clock used in all non-test contexts to measure elapsed time. Uses `{@link
 * System#nanoTime()}.
 */
final class DefaultElapsedClock implements Clock {
  /**
   * Returns current timestamp in nanoseconds.
   *
   * @return current timestamp in nanoseconds.
   */
  @Override
  public long getCurrentTimeNanos() {
    return System.nanoTime();
  }

  /** Default clock. */
  public static final Clock DEFAULT = new DefaultElapsedClock();
}
