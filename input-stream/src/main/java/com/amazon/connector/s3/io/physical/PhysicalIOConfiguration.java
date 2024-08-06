package com.amazon.connector.s3.io.physical;

import static com.amazon.connector.s3.util.Constants.ONE_KB;
import static com.amazon.connector.s3.util.Constants.ONE_MB;

import com.amazon.connector.s3.common.Preconditions;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/** Configuration for {@link PhysicalIO} */
@Getter
@Builder
@EqualsAndHashCode
public class PhysicalIOConfiguration {
  public static final int DEFAULT_CAPACITY_BLOB_STORE = 50;
  public static final int DEFAULT_CAPACITY_METADATA_STORE = 50;
  public static final boolean DEFAULT_USE_SINGLE_CACHE = true;
  public static final long DEFAULT_BLOCK_SIZE_BYTES = 8 * ONE_MB;
  public static final long DEFAULT_READ_AHEAD_BYTES = 64 * ONE_KB;
  public static final long DEFAULT_MAX_RANGE_SIZE = 8 * ONE_MB;
  public static final long DEFAULT_PART_SIZE = 8 * ONE_MB;

  /** Capacity, in blobs. {@link PhysicalIOConfiguration#DEFAULT_CAPACITY_BLOB_STORE} by default. */
  @Builder.Default private int blobStoreCapacity = DEFAULT_CAPACITY_BLOB_STORE;

  /**
   * Capacity, in blobs. {@link PhysicalIOConfiguration#DEFAULT_CAPACITY_METADATA_STORE} by default.
   */
  @Builder.Default private int metadataStoreCapacity = DEFAULT_CAPACITY_METADATA_STORE;

  /** Block size, in bytes. {@link PhysicalIOConfiguration#DEFAULT_BLOCK_SIZE_BYTES} by default. */
  @Builder.Default private long blockSizeBytes = DEFAULT_BLOCK_SIZE_BYTES;

  /** Read ahead, in bytes. {@link PhysicalIOConfiguration#DEFAULT_BLOCK_SIZE_BYTES} by default. */
  @Builder.Default private long readAheadBytes = DEFAULT_READ_AHEAD_BYTES;

  /**
   * Maximum range size, in bytes. {@link PhysicalIOConfiguration#DEFAULT_MAX_RANGE_SIZE} by
   * default.
   */
  @Builder.Default private long maxRangeSizeBytes = DEFAULT_MAX_RANGE_SIZE;

  /** Part size, in bytes. {@link PhysicalIOConfiguration#DEFAULT_PART_SIZE} by default. */
  @Builder.Default private long partSizeBytes = DEFAULT_PART_SIZE;

  /** Default set of settings for {@link PhysicalIO} */
  public static final PhysicalIOConfiguration DEFAULT = PhysicalIOConfiguration.builder().build();

  /**
   * Constructs {@link PhysicalIOConfiguration}.
   *
   * @param blobStoreCapacity The capacity of the BlobStore
   * @param metadataStoreCapacity The capacity of the MetadataStore
   * @param blockSizeBytes Block size, in bytes
   * @param readAheadBytes Read ahead, in bytes
   * @param maxRangeSizeBytes Maximum physical read issued against the object store
   * @param partSizeBytes What part size to use when splitting up logical reads
   */
  @Builder
  private PhysicalIOConfiguration(
      int blobStoreCapacity,
      int metadataStoreCapacity,
      long blockSizeBytes,
      long readAheadBytes,
      long maxRangeSizeBytes,
      long partSizeBytes) {
    Preconditions.checkArgument(blobStoreCapacity > 0, "`blobStoreCapacity` must be positive");
    Preconditions.checkArgument(
        metadataStoreCapacity > 0, "`metadataStoreCapacity` must be positive");
    Preconditions.checkArgument(blockSizeBytes > 0, "`blockSizeBytes` must be positive");
    Preconditions.checkArgument(readAheadBytes > 0, "`readAheadLengthBytes` must be positive");
    Preconditions.checkArgument(maxRangeSizeBytes > 0, "`maxRangeSize` must be positive");
    Preconditions.checkArgument(partSizeBytes > 0, "`partSize` must be positive");

    this.blobStoreCapacity = blobStoreCapacity;
    this.metadataStoreCapacity = metadataStoreCapacity;
    this.blockSizeBytes = blockSizeBytes;
    this.readAheadBytes = readAheadBytes;
    this.maxRangeSizeBytes = maxRangeSizeBytes;
    this.partSizeBytes = partSizeBytes;
  }
}
