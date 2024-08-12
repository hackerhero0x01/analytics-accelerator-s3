package com.amazon.connector.s3;

import com.amazon.connector.s3.common.telemetry.TelemetryConfiguration;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

/** Configuration for {@link S3SeekableInputStream} */
@Getter
@Builder
@EqualsAndHashCode
public class S3SeekableInputStreamConfiguration {
  @NonNull @Builder.Default
  private PhysicalIOConfiguration physicalIOConfiguration = PhysicalIOConfiguration.DEFAULT;

  @NonNull @Builder.Default
  private LogicalIOConfiguration logicalIOConfiguration = LogicalIOConfiguration.DEFAULT;

  @NonNull @Builder.Default
  private TelemetryConfiguration telemetryConfiguration = TelemetryConfiguration.DEFAULT;

  /** Default set of settings for {@link S3SeekableInputStream} */
  public static final S3SeekableInputStreamConfiguration DEFAULT =
      S3SeekableInputStreamConfiguration.builder().build();
}
