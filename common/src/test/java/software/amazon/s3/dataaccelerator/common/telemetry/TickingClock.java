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
package software.amazon.s3.dataaccelerator.common.telemetry;

import java.util.concurrent.atomic.AtomicLong;

/** This is a logical clock, that progresses only ahead and only does so when told. */
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
