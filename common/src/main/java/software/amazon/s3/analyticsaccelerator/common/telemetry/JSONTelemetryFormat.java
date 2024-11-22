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

import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link TelemetryFormat} to produce telemetry in JSON format.
 *
 * <p>This implementation renders logs in JSON. This can be useful when it comes to the analysis of
 * telemetry via automation and data analytics tools that understand JSON.
 *
 * <p>Operations are logged, e.g., as:
 *
 * <pre>{@code
 * {eventType":"start","timestamp":1730706857302000000,"id":-1ma4bbuhqkfp7,"name":"block.manager.make.range.available",
 * "generation":"3","thread_id":"266","range":"[41943044-50331651]","uri":"<snip>",
 * "range.effective":"[41943044-100663303]"}
 * ...
 * {eventType":"end","timestamp":1730706857202000000,"id":29bpvqqqr86q,"name":"block.get.async","generation":"1",
 * "thread_id":"266","range":"[19431832-22682496]","uri":"<snip>","elapsed":87699638}
 * }</pre>
 */
public class JSONTelemetryFormat implements TelemetryFormat {

  public static final String TELEMETRY_CONFIG_ID = "json";

  @Override
  public String renderDatapointMeasurement(
      TelemetryDatapointMeasurement datapointMeasurement, EpochFormatter epochFormatter) {
    return datapointMeasurement.toString(this, epochFormatter);
  }

  @Override
  public String renderMetricMeasurement(
      MetricMeasurement metricMeasurement, EpochFormatter epochFormatter) {
    // Start JSON
    StringBuilder sb = new StringBuilder("{");

    sb.append(String.format("\"timestamp\":%s,", metricMeasurement.getEpochTimestampNanos()));
    sb.append(String.format("\"metricName\":%s,", metricMeasurement.getMetric().getName()));
    sb.append(String.format("\"value\":%s", metricMeasurement.getValue()));

    // Add attributes if any
    if (!metricMeasurement.getMetric().getAttributes().isEmpty()) {
      sb.append(",");
      sb.append(renderTelemetryAttributes(metricMeasurement.getMetric().getAttributes()));
    }

    // Close JSON
    sb.append("}");
    return sb.toString();
  }

  @Override
  public String renderOperationStart(
      Operation operation, long epochTimestampNanos, EpochFormatter epochFormatter) {

    // Start JSON object
    StringBuilder sb = new StringBuilder("{");
    sb.append("\"eventType\":\"start\",");
    sb.append(String.format("\"timestamp\":%s,", epochTimestampNanos));
    sb.append(String.format("\"id\":\"%s\",", operation.getId()));

    // Add operation with attributes
    sb.append(renderOperation(operation));

    // Close JSON object
    sb.append("}");
    return sb.toString();
  }

  @Override
  public String renderOperationEnd(
      OperationMeasurement operationMeasurement, EpochFormatter epochFormatter) {
    // Start JSON object
    StringBuilder sb = new StringBuilder("{");
    sb.append("\"eventType\":\"end\",");
    sb.append(String.format("\"timestamp\":%s,", operationMeasurement.getEpochTimestampNanos()));

    // Add operation with attributes
    sb.append(renderOperation(operationMeasurement.getOperation()));

    // Append elapsed time
    sb.append(",");
    sb.append("\"elapsed\":").append(operationMeasurement.getElapsedTimeNanos());

    // Close JSON object
    sb.append("}");
    return sb.toString();
  }

  @Override
  public String renderOperation(Operation operation) {
    StringBuilder sb =
        new StringBuilder(
            String.format("\"id\":\"%s\",\"name\":\"%s\"", operation.getId(), operation.getName()));

    // Append attributes if any
    if (!operation.getAttributes().isEmpty()) {
      sb.append(",");
      sb.append(renderTelemetryAttributes(operation.getAttributes()));
    }

    return sb.toString();
  }

  @Override
  public String renderTelemetryAttributes(Map<String, Attribute> attributes) {
    return attributes.entrySet().stream()
        .map(
            mapEntry ->
                String.format("\"%s\":\"%s\"", mapEntry.getKey(), mapEntry.getValue().getValue()))
        .collect(Collectors.joining(","));
  }
}
