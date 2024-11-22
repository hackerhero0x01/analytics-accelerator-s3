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

import lombok.AllArgsConstructor;
import lombok.Getter;

/** This represents the metric measurement kind */
@Getter
@AllArgsConstructor
public enum MetricMeasurementKind {
  RAW(1), // raw data point, reported directly to telemetry
  AGGREGATE(2); // aggregate value, reported as a result of aggregation
  private final int value;
}
