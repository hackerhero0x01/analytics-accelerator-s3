/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.common.telemetry;

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
