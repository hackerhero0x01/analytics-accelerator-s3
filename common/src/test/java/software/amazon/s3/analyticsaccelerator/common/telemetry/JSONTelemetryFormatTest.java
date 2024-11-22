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

import static software.amazon.s3.analyticsaccelerator.CustomAssertions.assertContains;

import org.junit.jupiter.api.Test;

public class JSONTelemetryFormatTest {

  private final JSONTelemetryFormat format = new JSONTelemetryFormat();

  @Test
  void testRenderDatapointMeasurementWithoutAttributes() {
    // Given
    TelemetryDatapointMeasurement telemetryDatapointMeasurement =
        MetricMeasurement.builder()
            .epochTimestampNanos(123)
            .metric(Metric.builder().name("metric").build())
            .value(1337)
            .build();

    // When
    String output =
        format.renderDatapointMeasurement(telemetryDatapointMeasurement, EpochFormatter.DEFAULT);

    // Then
    assertContains(output, "{", "}");
    assertContains(output, "\"timestamp\":123");
    assertContains(output, "\"metricName\":metric");
    assertContains(output, "\"value\":1337.0");
  }

  @Test
  void testRenderDatapointMeasurementWithAttributes() {
    // Given
    TelemetryDatapointMeasurement telemetryDatapointMeasurement =
        MetricMeasurement.builder()
            .epochTimestampNanos(123)
            .metric(
                Metric.builder()
                    .name("metric")
                    .attribute(Attribute.of("attr1", "attrValue1"))
                    .attribute(Attribute.of("attr2", "attrValue2"))
                    .build())
            .value(1337)
            .build();

    // When
    String output =
        format.renderDatapointMeasurement(telemetryDatapointMeasurement, EpochFormatter.DEFAULT);

    // Then
    assertContains(output, "{", "}");
    assertContains(output, "\"timestamp\":123");
    assertContains(output, "\"metricName\":metric");
    assertContains(output, "\"value\":1337.0");
    assertContains(output, "\"attr1\":\"attrValue1\"");
    assertContains(output, "\"attr2\":\"attrValue2\"");
  }

  @Test
  void testRenderOperationStart() {
    // Given
    Operation operation = Operation.builder().name("op").id("test.id").build();

    // When
    String output = format.renderOperationStart(operation, 1234, EpochFormatter.DEFAULT);

    // Then
    assertContains(output, "{", "}");
    assertContains(output, "\"eventType\":\"start\"");
    assertContains(output, "\"id\":\"test.id\"");
  }

  @Test
  void testRenderOperationEnd() {
    // Given
    Operation operation =
        Operation.builder()
            .name("op")
            .id("test.id")
            .attribute("a1", "v1")
            .attribute("a2", "v2")
            .build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .epochTimestampNanos(123452163)
            .elapsedStartTimeNanos(123)
            .elapsedCompleteTimeNanos(456)
            .level(TelemetryLevel.STANDARD)
            .build();

    // When
    String output = format.renderOperationEnd(operationMeasurement, EpochFormatter.DEFAULT);

    // Then
    assertContains(output, "{", "}");
    assertContains(output, "\"eventType\":\"end\""); // operation type must be end
    assertContains(output, "\"id\":\"test.id\"");
    assertContains(output, "\"a1\":\"v1\""); // attributes must be present
    assertContains(output, "\"a2\":\"v2\"");
    assertContains(output, "\"elapsed\":333"); // the elapsed time must be calculated
  }
}
