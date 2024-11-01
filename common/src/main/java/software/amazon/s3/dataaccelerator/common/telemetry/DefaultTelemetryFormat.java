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

import java.util.Map;
import lombok.NonNull;

/**
 * A {@link TelemetryFormat} to produce human-readable telemetry log traces.
 *
 * <p>This implementation produces logs in the default format. This is mainly useful for operators
 * and developers to quickly understand software behaviour. For more structured telemetry formats
 * check the other implementations, like JSONTelemetryFormat.
 *
 * <p>Operations are logged, e.g., as:
 *
 * <pre>{@code
 * [2024-11-01T08:18:49.745Z] [  start] [1ive<--hpbi] block.get.async(thread_id=267, uri=s3://bucket/key)
 * ...
 * [2024-11-01T08:18:44.988Z] [success] [1ive<--hpbi] block.get.async(generation=0, thread_id=267,
 * range=[785237-1772252], uri=s3://bucket/key): 238,830,003 ns
 * }</pre>
 *
 * <p>Metrics are logged, e.g., as:
 *
 * <pre>{@code
 * [2024-11-01T08:50:21.409Z] s3.client.head.max: 55,663,750.00
 * }</pre>
 */
public class DefaultTelemetryFormat implements TelemetryFormat {

  public static final String TELEMETRY_CONFIG_ID = "default"; // this is the default implementation

  private static final String METRIC_FORMAT_STRING = "[%s] %s: %,.2f";

  @Override
  public String renderDatapointMeasurement(
      @NonNull TelemetryDatapointMeasurement datapointMeasurement,
      @NonNull EpochFormatter epochFormatter) {
    return datapointMeasurement.toString(this, epochFormatter);
  }

  @Override
  public String renderMetricMeasurement(
      @NonNull MetricMeasurement metricMeasurement, @NonNull EpochFormatter epochFormatter) {
    return String.format(
        METRIC_FORMAT_STRING,
        epochFormatter.formatNanos(metricMeasurement.getEpochTimestampNanos()),
        renderMetric(metricMeasurement.getMetric()),
        metricMeasurement.getValue());
  }

  private static final String OPERATION_START_FORMAT_STRING = "[%s] [  start] %s";
  private static final String OPERATION_ERROR_FORMAT_STRING = " [%s: '%s']";
  public static final String OPERATION_COMPLETE_FORMAT_STRING = "[%s] [%s] %s: %,d ns";

  @Override
  public String renderOperationStart(
      @NonNull Operation operation,
      long epochTimestampNanos,
      @NonNull EpochFormatter epochFormatter) {
    return String.format(
        OPERATION_START_FORMAT_STRING,
        epochFormatter.formatNanos(epochTimestampNanos),
        renderOperation(operation));
  }

  private static final String SUCCESS = "success";
  private static final String FAILURE = "failure";

  @Override
  public String renderOperationEnd(
      @NonNull OperationMeasurement operationMeasurement, @NonNull EpochFormatter epochFormatter) {
    String result =
        String.format(
            OPERATION_COMPLETE_FORMAT_STRING,
            epochFormatter.formatNanos(operationMeasurement.getEpochTimestampNanos()),
            operationMeasurement.succeeded() ? SUCCESS : FAILURE,
            renderOperation(operationMeasurement.getOperation()),
            operationMeasurement.getElapsedTimeNanos());

    if (operationMeasurement.getError().isPresent()) {
      result +=
          String.format(
              OPERATION_ERROR_FORMAT_STRING,
              operationMeasurement.getError().get().getClass().getCanonicalName(),
              operationMeasurement.getError().get().getMessage());
    }
    return result;
  }

  @Override
  public String renderOperation(Operation operation) {
    StringBuilder stringBuilder = new StringBuilder();
    // id
    stringBuilder.append("[");
    stringBuilder.append(operation.getId());

    // add parent id, if present
    operation
        .getParent()
        .ifPresent(
            parent -> {
              stringBuilder.append("<-");
              stringBuilder.append(parent.getId());
            });
    stringBuilder.append("] ");

    // name
    stringBuilder.append(operation.getName());

    // attributes
    stringBuilder.append(renderTelemetryAttributes(operation.getAttributes()));

    return stringBuilder.toString();
  }

  @Override
  public String renderTelemetryAttributes(Map<String, Attribute> attributes) {
    StringBuilder stringBuilder = new StringBuilder();
    if (!attributes.isEmpty()) {
      stringBuilder.append("(");
      int count = 0;
      for (Attribute attribute : attributes.values()) {
        stringBuilder.append(attribute.getName());
        stringBuilder.append("=");
        stringBuilder.append(attribute.getValue());
        if (++count != attributes.size()) {
          stringBuilder.append(", ");
        }
      }
      stringBuilder.append(")");
    }
    return stringBuilder.toString();
  }

  private String renderMetric(Metric metric) {
    StringBuilder sb = new StringBuilder();
    sb.append(metric.getName());
    sb.append(renderTelemetryAttributes(metric.getAttributes()));
    return sb.toString();
  }
}
