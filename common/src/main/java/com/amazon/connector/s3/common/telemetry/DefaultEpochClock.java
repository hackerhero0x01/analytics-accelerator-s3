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
