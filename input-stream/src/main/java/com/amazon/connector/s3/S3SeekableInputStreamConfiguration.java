package com.amazon.connector.s3;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManagerConfiguration;
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
  private BlockManagerConfiguration blockManagerConfiguration = BlockManagerConfiguration.DEFAULT;

  @Builder.Default
  private LogicalIOConfiguration logicalIOConfiguration = LogicalIOConfiguration.DEFAULT;

  /** Default set of settings for {@link S3SeekableInputStream} */
  public static final S3SeekableInputStreamConfiguration DEFAULT =
      S3SeekableInputStreamConfiguration.builder().build();

  /**
   * Creates a new instance of
   *
   * @param blockManagerConfiguration - {@link BlockManagerConfiguration}
   */
  private S3SeekableInputStreamConfiguration(
      @NonNull BlockManagerConfiguration blockManagerConfiguration,
      @NonNull LogicalIOConfiguration logicalIOConfiguration) {
    this.blockManagerConfiguration = blockManagerConfiguration;
    this.logicalIOConfiguration = logicalIOConfiguration;
  }
}
