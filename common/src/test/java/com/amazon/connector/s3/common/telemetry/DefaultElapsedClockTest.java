package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class DefaultElapsedClockTest {
  @Test
  void testClockTicksForward() {
    Clock clock = new DefaultElapsedClock();
    long timeStamp1 = clock.getCurrentTimeNanos();
    long timeStamp2 = clock.getCurrentTimeNanos();
    assertTrue(timeStamp1 <= timeStamp2);
  }

  @Test
  void testClockTicksForwardForDefault() {
    Clock clock = DefaultElapsedClock.DEFAULT;
    long timeStamp1 = clock.getCurrentTimeNanos();
    long timeStamp2 = clock.getCurrentTimeNanos();
    assertTrue(timeStamp1 <= timeStamp2);
  }
}
