package com.amazon.connector.s3.common.telemetry;

/**
 * Represents a clock that can be used to measure time. The assumption is that the time grows
 * monotonically, in nanoseconds.
 */
interface Clock {
  /**
   * Returns current timestamp in nanoseconds.
   *
   * @return current timestamp in nanoseconds.
   */
  long getCurrentTimeNanos();
}
