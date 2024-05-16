package com.amazon.connector.s3;

import com.amazon.connector.s3.blockmanager.BlockManagerConfiguration;
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
  private final BlockManagerConfiguration blockManagerConfiguration =
      BlockManagerConfiguration.DEFAULT;

  /** Default set of settings for {@link S3SeekableInputStream} */
  public static final S3SeekableInputStreamConfiguration DEFAULT =
      S3SeekableInputStreamConfiguration.builder().build();

  /**
   * Creates a new instance of
   *
   * @param blockManagerConfiguration - {@link BlockManagerConfiguration}
   */
  public S3SeekableInputStreamConfiguration(
      @NonNull BlockManagerConfiguration blockManagerConfiguration) {
    this.blockManagerConfiguration = blockManagerConfiguration;
  }
}
