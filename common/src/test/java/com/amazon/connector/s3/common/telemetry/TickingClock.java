package com.amazon.connector.s3.common.telemetry;

import java.util.concurrent.atomic.AtomicLong;

/** It's basically a Lamport Clock, that progresses only ahead and only does so when told. */
class TickingClock implements Clock {
  private final AtomicLong ticks = new AtomicLong();

  /**
   * Creates the clock with the given tick count
   *
   * @param ticks - initial tick count.
   */
  public TickingClock(long ticks) {
    this.ticks.set(ticks);
  }

  /**
   * Moves the clock forward
   *
   * @param ticks - tick count.
   * @return total ticks
   */
  public long tick(long ticks) {
    return this.ticks.addAndGet(ticks);
  }

  /**
   * Current ticks.
   *
   * @return current ticks.
   */
  @Override
  public long getCurrentTimeNanos() {
    return ticks.get();
  }
}
