package com.amazon.connector.s3;

import com.amazon.connector.s3.common.Configuration;
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
  @Builder.Default
  private PhysicalIOConfiguration physicalIOConfiguration = PhysicalIOConfiguration.DEFAULT;

  @Builder.Default
  private LogicalIOConfiguration logicalIOConfiguration = LogicalIOConfiguration.DEFAULT;

  /** Default set of settings for {@link S3SeekableInputStream} */
  public static final S3SeekableInputStreamConfiguration DEFAULT =
      S3SeekableInputStreamConfiguration.builder().build();

  /**
   * Constructs {@link S3SeekableInputStream} from {@link Configuration} object.
   *
   * @param configuration Configuration object to generate S3SeekableInputStreamConfiguration from
   * @return S3SeekableInputStreamConfiguration
   */
  public static S3SeekableInputStreamConfiguration fromConfiguration(Configuration configuration) {
    return S3SeekableInputStreamConfiguration.builder()
        .physicalIOConfiguration(PhysicalIOConfiguration.fromConfiguration(configuration))
        .logicalIOConfiguration(LogicalIOConfiguration.fromConfiguration(configuration))
        .build();
  }

  /**
   * Creates a new instance of
   *
   * @param physicalIOConfiguration - {@link PhysicalIOConfiguration} configuration
   * @param logicalIOConfiguration - {@link LogicalIOConfiguration} configuration
   */
  private S3SeekableInputStreamConfiguration(
      @NonNull PhysicalIOConfiguration physicalIOConfiguration,
      @NonNull LogicalIOConfiguration logicalIOConfiguration) {
    this.physicalIOConfiguration = physicalIOConfiguration;
    this.logicalIOConfiguration = logicalIOConfiguration;
  }
}
