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

/**
 * An interface defining what formatting telemetry means.
 *
 * <p>It is essentially a collection of `toString`-like methods that are capable of _rendering_
 * metric measurements, start and end of operations and other measurements into a textual
 * representation.
 */
public interface TelemetryFormat {

  /**
   * Given a {@link TelemetryDatapointMeasurement}, returns its String representation.
   *
   * @param datapointMeasurement the {@link TelemetryDatapointMeasurement} instance to render
   * @param epochFormatter the {@link EpochFormatter} that controls the formatting of timestamps
   * @return a string representation of the {@link TelemetryDatapointMeasurement}
   */
  String renderDatapointMeasurement(
      TelemetryDatapointMeasurement datapointMeasurement, EpochFormatter epochFormatter);

  /**
   * Given a {@link MetricMeasurement}, returns its String representation.
   *
   * @param metricMeasurement the {@link MetricMeasurement} instance to render
   * @param epochFormatter the {@link EpochFormatter} that controls the formatting of timestamps
   * @return a string representation of the {@link MetricMeasurement}
   */
  String renderMetricMeasurement(
      MetricMeasurement metricMeasurement, EpochFormatter epochFormatter);

  /**
   * Given an {@link Operation}, returns its String representation.
   *
   * @param operation the {@link Operation} instance to render
   * @param epochTimestampNanos the epoch timestamp when the {@link Operation} started in
   *     nanoseconds
   * @param epochFormatter the {@link EpochFormatter} to use to render timestamps
   * @return a string representation of the {@link Operation}
   */
  String renderOperationStart(
      Operation operation, long epochTimestampNanos, EpochFormatter epochFormatter);

  /**
   * Given an {@link OperationMeasurement}, returns its String representation.
   *
   * @param operationMeasurement the {@link OperationMeasurement} instance to render
   * @param epochFormatter the {@link EpochFormatter} to use to render timestamps
   * @return a string representation of the {@link OperationMeasurement}
   */
  String renderOperationEnd(
      OperationMeasurement operationMeasurement, EpochFormatter epochFormatter);
}
