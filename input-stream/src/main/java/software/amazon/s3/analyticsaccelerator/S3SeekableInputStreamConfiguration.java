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
package software.amazon.s3.analyticsaccelerator;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.common.telemetry.TelemetryConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;

/** Configuration for {@link S3SeekableInputStream} */
@Getter
@Builder
@EqualsAndHashCode
public class S3SeekableInputStreamConfiguration {
  public static final String PHYSICAL_IO_PREFIX = "physicalio";
  public static final String LOGICAL_IO_PREFIX = "logicalio";
  public static final String TELEMETRY_PREFIX = "telemetry";

  @Builder.Default @NonNull private PhysicalIOConfiguration physicalIOConfiguration = PhysicalIOConfiguration.DEFAULT;

  @Builder.Default @NonNull private LogicalIOConfiguration logicalIOConfiguration = LogicalIOConfiguration.DEFAULT;

  @Builder.Default @NonNull private TelemetryConfiguration telemetryConfiguration = TelemetryConfiguration.DEFAULT;

  /** Default set of settings for {@link S3SeekableInputStream} */
  public static final S3SeekableInputStreamConfiguration DEFAULT =
      S3SeekableInputStreamConfiguration.builder().build();

  /**
   * Constructs {@link S3SeekableInputStream} from {@link ConnectorConfiguration} object.
   *
   * @param configuration Configuration object to generate S3SeekableInputStreamConfiguration from
   * @return S3SeekableInputStreamConfiguration
   */
  public static S3SeekableInputStreamConfiguration fromConfiguration(
      ConnectorConfiguration configuration) {
    return S3SeekableInputStreamConfiguration.builder()
        .physicalIOConfiguration(
            PhysicalIOConfiguration.fromConfiguration(configuration.map(PHYSICAL_IO_PREFIX)))
        .logicalIOConfiguration(
            LogicalIOConfiguration.fromConfiguration(configuration.map(LOGICAL_IO_PREFIX)))
        .telemetryConfiguration(
            TelemetryConfiguration.fromConfiguration(configuration.map(TELEMETRY_PREFIX)))
        .build();
  }

  @Override
  public String toString() {
    final StringBuilder dump = new StringBuilder();

    dump.append(physicalIOConfiguration);
    dump.append(logicalIOConfiguration);
    dump.append(telemetryConfiguration);

    return dump.toString();
  }
}
