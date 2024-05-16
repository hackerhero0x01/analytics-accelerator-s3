package com.amazon.connector.s3.blockmanager;

import static com.amazon.connector.s3.util.Constants.ONE_KB;
import static com.amazon.connector.s3.util.Constants.ONE_MB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class BlockManagerConfigurationTest {
  @Test
  void testDefaultBuilder() {
    BlockManagerConfiguration configuration = BlockManagerConfiguration.builder().build();
    assertEquals(
        configuration.getBlockSizeBytes(), BlockManagerConfiguration.DEFAULT_BLOCK_SIZE_BYTES);
    assertEquals(
        configuration.getCapacityBlocks(), BlockManagerConfiguration.DEFAULT_CAPACITY_BLOCKS);
    assertEquals(
        configuration.getReadAheadBytes(), BlockManagerConfiguration.DEFAULT_READ_AHEAD_BYTES);
  }

  @Test
  void testDefault() {
    assertEquals(BlockManagerConfiguration.builder().build(), BlockManagerConfiguration.DEFAULT);
  }

  @Test
  void testNonDefaults() {
    BlockManagerConfiguration configuration =
        BlockManagerConfiguration.builder()
            .blockSizeBytes(4 * ONE_MB)
            .capacityBlocks(20)
            .readAheadBytes(128 * ONE_KB)
            .build();
    assertEquals(configuration.getBlockSizeBytes(), 4 * ONE_MB);
    assertEquals(configuration.getCapacityBlocks(), 20);
    assertEquals(configuration.getReadAheadBytes(), 128 * ONE_KB);
  }

  @Test
  void testInvalidCapacityBlocks() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            BlockManagerConfiguration.builder()
                .blockSizeBytes(4 * ONE_MB)
                .capacityBlocks(-10)
                .readAheadBytes(128 * ONE_KB)
                .build());
  }

  @Test
  void testInvalidBlockSizeBytes() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            BlockManagerConfiguration.builder()
                .blockSizeBytes(-4 * ONE_MB)
                .capacityBlocks(20)
                .readAheadBytes(128 * ONE_KB)
                .build());
  }

  @Test
  void testInvalidReadAheadLengthBytes() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            BlockManagerConfiguration.builder()
                .blockSizeBytes(4 * ONE_MB)
                .capacityBlocks(20)
                .readAheadBytes(-128 * ONE_KB)
                .build());
  }
}
