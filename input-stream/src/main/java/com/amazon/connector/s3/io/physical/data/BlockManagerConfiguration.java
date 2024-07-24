package com.amazon.connector.s3.io.physical.data;

import static com.amazon.connector.s3.util.Constants.ONE_KB;
import static com.amazon.connector.s3.util.Constants.ONE_MB;

import com.amazon.connector.s3.common.Preconditions;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Configuration for {@link BlockManager} */
@Getter
@Builder
@EqualsAndHashCode
public class BlockManagerConfiguration {
  public static final int DEFAULT_CAPACITY_BLOCKS = 10;
  public static final int DEFAULT_CAPACITY_MULTI_OBJECTS = 45;
  public static final int DEFAULT_CAPACITY_PREFETCH_CACHE = 45;
  public static final boolean DEFAULT_USE_SINGLE_CACHE = true;
  public static final long DEFAULT_BLOCK_SIZE_BYTES = 8 * ONE_MB;
  public static final long DEFAULT_READ_AHEAD_BYTES = 64 * ONE_KB;

  /** Capacity, in blocks. {@link BlockManagerConfiguration#DEFAULT_CAPACITY_BLOCKS} by default. */
  @Builder.Default private int capacityBlocks = DEFAULT_CAPACITY_BLOCKS;

  /**
   * Capacity, in objects {@link BlockManagerConfiguration#DEFAULT_CAPACITY_MULTI_OBJECTS} by
   * default
   */
  @Builder.Default private int capacityMultiObjects = DEFAULT_CAPACITY_MULTI_OBJECTS;

  /**
   * Capacity, in objects {@link BlockManagerConfiguration#DEFAULT_CAPACITY_PREFETCH_CACHE} by
   * default
   */
  @Builder.Default private int capacityPrefetchCache = DEFAULT_CAPACITY_PREFETCH_CACHE;

  /** Use single cache. {@link BlockManagerConfiguration#DEFAULT_USE_SINGLE_CACHE} by default. */
  @Builder.Default private boolean useSingleCache = DEFAULT_USE_SINGLE_CACHE;

  /**
   * Block size, in bytes. {@link BlockManagerConfiguration#DEFAULT_BLOCK_SIZE_BYTES} by default.
   */
  @Builder.Default private long blockSizeBytes = DEFAULT_BLOCK_SIZE_BYTES;

  /**
   * Read ahead, in bytes. {@link BlockManagerConfiguration#DEFAULT_BLOCK_SIZE_BYTES} by default.
   */
  @Builder.Default private long readAheadBytes = DEFAULT_READ_AHEAD_BYTES;

  /** Default set of settings for {@link BlockManager} */
  public static final BlockManagerConfiguration DEFAULT =
      BlockManagerConfiguration.builder().build();

  /**
   * Constructs {@link BlockManagerConfiguration}.
   *
   * @param capacityBlocks Capacity, in blocks.
   * @param blockSizeBytes Block size, in bytes
   * @param readAheadBytes Read ahead, in bytes
   * @param useSingleCache Use single cache
   * @param capacityMultiObjects Capacity, in objects
   * @param capacityPrefetchCache Capacity, of the prefetch cache
   */
  @Builder
  private BlockManagerConfiguration(
      int capacityBlocks,
      int capacityMultiObjects,
      int capacityPrefetchCache,
      boolean useSingleCache,
      long blockSizeBytes,
      long readAheadBytes) {
    Preconditions.checkArgument(capacityBlocks > 0, "`capacityBlocks` must be positive");
    Preconditions.checkArgument(
        capacityMultiObjects > 0, " `capacityMultiObjects` must be positive");
    Preconditions.checkArgument(
        capacityPrefetchCache > 0, " `capacityPrefetchCache` must be positive");
    Preconditions.checkArgument(
        capacityMultiObjects > 0, "`capacityMultiObjects` must be positive");
    Preconditions.checkArgument(blockSizeBytes > 0, "`blockSizeBytes` must be positive");
    Preconditions.checkArgument(readAheadBytes > 0, "`readAheadLengthBytes` must be positive");

    this.capacityBlocks = capacityBlocks;
    this.capacityPrefetchCache = capacityPrefetchCache;
    this.capacityMultiObjects = capacityMultiObjects;
    this.blockSizeBytes = blockSizeBytes;
    this.readAheadBytes = readAheadBytes;
    this.useSingleCache = useSingleCache;
  }
}
