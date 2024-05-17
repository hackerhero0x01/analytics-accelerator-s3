package com.amazon.connector.s3.blockmanager;

import static com.amazon.connector.s3.util.Constants.ONE_KB;
import static com.amazon.connector.s3.util.Constants.ONE_MB;

import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Configuration for {@link BlockManager} */
@Getter
@Builder
@EqualsAndHashCode
public class BlockManagerConfiguration {
  public static final int DEFAULT_CAPACITY_BLOCKS = 10;
  public static final long DEFAULT_BLOCK_SIZE_BYTES = 8 * ONE_MB;
  public static final long DEFAULT_READ_AHEAD_BYTES = 64 * ONE_KB;

  /** Capacity, in blocks. {@link BlockManagerConfiguration#DEFAULT_CAPACITY_BLOCKS} by default. */
  @Builder.Default private final int capacityBlocks = DEFAULT_CAPACITY_BLOCKS;

  /**
   * Block size, in bytes. {@link BlockManagerConfiguration#DEFAULT_BLOCK_SIZE_BYTES} by default.
   */
  @Builder.Default private final long blockSizeBytes = DEFAULT_BLOCK_SIZE_BYTES;

  /**
   * Read ahead, in bytes. {@link BlockManagerConfiguration#DEFAULT_BLOCK_SIZE_BYTES} by default.
   */
  @Builder.Default private final long readAheadBytes = DEFAULT_READ_AHEAD_BYTES;

  /** Default set of settings for {@link BlockManager} */
  public static final BlockManagerConfiguration DEFAULT =
      BlockManagerConfiguration.builder().build();

  /**
   * Constructs {@link BlockManagerConfiguration}.
   *
   * @param capacityBlocks Capacity, in blocks.
   * @param blockSizeBytes Block size, in bytes
   * @param readAheadBytes Read ahead, in bytes
   */
  @Builder
  private BlockManagerConfiguration(int capacityBlocks, long blockSizeBytes, long readAheadBytes) {
    Preconditions.checkArgument(capacityBlocks > 0, "`capacityBlocks` must be positive");
    Preconditions.checkArgument(blockSizeBytes > 0, "`blockSizeBytes` must be positive");
    Preconditions.checkArgument(readAheadBytes > 0, "`readAheadLengthBytes` must be positive");

    this.capacityBlocks = capacityBlocks;
    this.blockSizeBytes = blockSizeBytes;
    this.readAheadBytes = readAheadBytes;
  }
}
