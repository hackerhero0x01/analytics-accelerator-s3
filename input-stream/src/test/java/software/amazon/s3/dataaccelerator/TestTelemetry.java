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
package software.amazon.s3.dataaccelerator;

import software.amazon.s3.dataaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.dataaccelerator.common.telemetry.TelemetryConfiguration;
import software.amazon.s3.dataaccelerator.common.telemetry.TelemetryLevel;

public final class TestTelemetry {
  private TestTelemetry() {}

  /**
   * Default telemetry to use for tests - combination of NoOp telemetry with VERBOSE level provides
   * the best code coverage
   */
  public static final Telemetry DEFAULT =
      Telemetry.createTelemetry(
          TelemetryConfiguration.builder()
              .loggingEnabled(false)
              .stdOutEnabled(false)
              .level(TelemetryLevel.VERBOSE.toString())
              .build());
}
