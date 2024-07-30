package com.amazon.connector.s3.io.physical.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.request.ReadMode;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import java.util.Optional;
import java.util.OptionalLong;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class BlockStoreTest {

  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  public void test__blockStore__getBlockAfterAddBlock() {
    // Given: empty BlockStore
    FakeObjectClient fakeObjectClient = new FakeObjectClient("test-data");
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, PhysicalIOConfiguration.DEFAULT);
    BlockStore blockStore = new BlockStore(TEST_URI, metadataStore);

    // When: a new block is added
    blockStore.add(
        new Block(
            TEST_URI, fakeObjectClient, 3, 5, 0, ReadMode.SYNC, PhysicalIOConfiguration.DEFAULT));

    // Then: getBlock can retrieve the same block
    Optional<Block> b = blockStore.getBlock(4);

    assertTrue(b.isPresent());
    assertEquals(b.get().getStart(), 3);
    assertEquals(b.get().getEnd(), 5);
    assertEquals(b.get().getGeneration(), 0);
  }

  @Test
  @SneakyThrows
  public void test__blockStore__findNextMissingByteCorrect() {
    // Given: BlockStore with blocks (2,3), (5,10), (12,15)
    final String X_TIMES_16 = "xxxxxxxxxxxxxxxx";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(X_TIMES_16);
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, PhysicalIOConfiguration.DEFAULT);
    BlockStore blockStore = new BlockStore(TEST_URI, metadataStore);

    blockStore.add(
        new Block(
            TEST_URI, fakeObjectClient, 2, 3, 0, ReadMode.SYNC, PhysicalIOConfiguration.DEFAULT));
    blockStore.add(
        new Block(
            TEST_URI, fakeObjectClient, 5, 10, 0, ReadMode.SYNC, PhysicalIOConfiguration.DEFAULT));
    blockStore.add(
        new Block(
            TEST_URI, fakeObjectClient, 12, 15, 0, ReadMode.SYNC, PhysicalIOConfiguration.DEFAULT));

    // When & Then: we query for the next missing byte, the result is correct
    assertEquals(OptionalLong.of(0), blockStore.findNextMissingByte(0));
    assertEquals(OptionalLong.of(1), blockStore.findNextMissingByte(1));
    assertEquals(OptionalLong.of(4), blockStore.findNextMissingByte(2));
    assertEquals(OptionalLong.of(4), blockStore.findNextMissingByte(3));
    assertEquals(OptionalLong.of(4), blockStore.findNextMissingByte(4));
    assertEquals(OptionalLong.of(11), blockStore.findNextMissingByte(5));
    assertEquals(OptionalLong.of(11), blockStore.findNextMissingByte(11));
    assertEquals(OptionalLong.empty(), blockStore.findNextMissingByte(14));
  }

  @Test
  public void test__blockStore__findNextAvailableByteCorrect() {
    // Given: BlockStore with blocks (2,3), (5,10), (12,15)
    final String X_TIMES_16 = "xxxxxxxxxxxxxxxx";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(X_TIMES_16);
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, PhysicalIOConfiguration.DEFAULT);
    BlockStore blockStore = new BlockStore(TEST_URI, metadataStore);

    blockStore.add(
        new Block(
            TEST_URI, fakeObjectClient, 2, 3, 0, ReadMode.SYNC, PhysicalIOConfiguration.DEFAULT));
    blockStore.add(
        new Block(
            TEST_URI, fakeObjectClient, 5, 10, 0, ReadMode.SYNC, PhysicalIOConfiguration.DEFAULT));
    blockStore.add(
        new Block(
            TEST_URI, fakeObjectClient, 12, 15, 0, ReadMode.SYNC, PhysicalIOConfiguration.DEFAULT));

    // When & Then: we query for the next available byte, the result is correct
    assertEquals(OptionalLong.of(2), blockStore.findNextLoadedByte(0));
    assertEquals(OptionalLong.of(2), blockStore.findNextLoadedByte(1));
    assertEquals(OptionalLong.of(2), blockStore.findNextLoadedByte(2));
    assertEquals(OptionalLong.of(3), blockStore.findNextLoadedByte(3));
    assertEquals(OptionalLong.of(5), blockStore.findNextLoadedByte(4));
    assertEquals(OptionalLong.of(5), blockStore.findNextLoadedByte(5));
    assertEquals(OptionalLong.of(12), blockStore.findNextLoadedByte(11));
    assertEquals(OptionalLong.of(15), blockStore.findNextLoadedByte(15));
  }

  @Test
  public void test__blockStore__closesBlocks() {
    // Given: BlockStore with a block
    BlockStore blockStore = new BlockStore(TEST_URI, mock(MetadataStore.class));
    Block block = mock(Block.class);
    blockStore.add(block);

    // When: blockStore is closed
    blockStore.close();

    // Then: underlying block is also closed
    verify(block, times(1)).close();
  }

  @Test
  public void test__blockStore__closeWorksWithExceptions() {
    // Given: BlockStore with two blocks
    BlockStore blockStore = new BlockStore(TEST_URI, mock(MetadataStore.class));
    Block b1 = mock(Block.class);
    Block b2 = mock(Block.class);
    blockStore.add(b1);
    blockStore.add(b2);

    // When: b1 throws when closed
    doThrow(new RuntimeException("something horrible")).when(b1).close();
    blockStore.close();

    // Then: 1\ blockStore.close did not throw, 2\ b2 was closed
    verify(b2, times(1)).close();
  }
}
