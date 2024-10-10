  /*
   * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   *
   * Licensed under the Apache License, Version 2.0 (the "License").
   * You may not use this file except in compliance with the License.
   * You may obtain a copy of the License at
   *
   *   http://www.apache.org/licenses/LICENSE-2.0
  
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
package com.amazon.connector.s3.common.telemetry;

import java.io.Closeable;

/** Interface that represents a telemetry reporter. */
public interface TelemetryReporter extends Closeable {
  /**
   * Reports the start of an operation
   *
   * @param epochTimestampNanos wall clock time for the operation start
   * @param operation and instance of {@link Operation} to start
   */
  void reportStart(long epochTimestampNanos, Operation operation);

  /**
   * Reports the completion of an operation
   *
   * @param datapointMeasurement an instance of {@link TelemetryDatapointMeasurement}.
   */
  void reportComplete(TelemetryDatapointMeasurement datapointMeasurement);

  /** Flushes any intermediate state of the reporters */
  void flush();

  /**
   * Default implementation of {@link AutoCloseable#close()}, that calls {@link
   * TelemetryReporter#flush()}
   */
  default void close() {
    this.flush();
  }
}
