package com.amazon.connector.s3.io.physical.blockmanager;

import static com.amazon.connector.s3.util.Constants.ONE_KB;
import static com.amazon.connector.s3.util.Constants.ONE_MB;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.util.io.IOUtil.close;

import com.amazon.connector.s3.util.S3URI;
import org.junit.jupiter.api.Test;

import java.io.IOException;

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
            .capacityPrefetchCache(30)
            .build();
    assertEquals(configuration.getBlockSizeBytes(), 4 * ONE_MB);
    assertEquals(configuration.getCapacityBlocks(), 20);
    assertEquals(configuration.getReadAheadBytes(), 128 * ONE_KB);
    assertEquals(configuration.getCapacityPrefetchCache(), 30);
  }

  @Test
  void testInvalidCapacityPrefetchCache() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            BlockManagerConfiguration.builder()
                .blockSizeBytes(4 * ONE_MB)
                .capacityBlocks(20)
                .readAheadBytes(128 * ONE_KB)
                .capacityPrefetchCache(-10)
                .build());

    assertThrows(
            IllegalArgumentException.class,
            () ->
                    BlockManagerConfiguration.builder()
                            .blockSizeBytes(4 * ONE_MB)
                            .capacityMultiObjects(-20)
                            .readAheadBytes(128 * ONE_KB)
                            .capacityPrefetchCache(11)
                            .build());
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

  @Test
  void testClose() throws IOException {
    MultiObjectsBlockManager multiObjectsBlockManager = mock(MultiObjectsBlockManager.class);
    BlockManager blockManager = new BlockManager(multiObjectsBlockManager, S3URI.of("test", "test"));
    assertDoesNotThrow( () -> blockManager.close());
    verify(multiObjectsBlockManager, times(0)).close();
  }
}
