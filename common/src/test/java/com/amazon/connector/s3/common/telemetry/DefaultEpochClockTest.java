package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class DefaultEpochClockTest {
  @Test
  void testClockIsAlignedWithWallTime() {
    Clock clock = new DefaultEpochClock();
    long before = System.currentTimeMillis();
    long now = TimeUnit.NANOSECONDS.toMillis(clock.getCurrentTimeNanos());
    long after = System.currentTimeMillis();
    assertTrue(before <= now);
    assertTrue(now <= after);
  }

  @Test
  void testClockIsAlignedWithWallTimeForDefault() {
    Clock clock = DefaultEpochClock.DEFAULT;
    long before = System.currentTimeMillis();
    long now = TimeUnit.NANOSECONDS.toMillis(clock.getCurrentTimeNanos());
    long after = System.currentTimeMillis();
    assertTrue(before <= now);
    assertTrue(now <= after);
  }
}
