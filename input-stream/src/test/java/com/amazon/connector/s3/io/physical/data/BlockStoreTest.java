package com.amazon.connector.s3.io.physical.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import java.util.Optional;
import java.util.OptionalLong;
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
    blockStore.add(new Block(TEST_URI, fakeObjectClient, 3, 5, 0));

    // Then: getBlock can retrieve the same block
    Optional<Block> b = blockStore.getBlock(4);

    assertTrue(b.isPresent());
    assertEquals(b.get().getStart(), 3);
    assertEquals(b.get().getEnd(), 5);
    assertEquals(b.get().getGeneration(), 0);
  }

  @Test
  public void test__blockStore__findNextMissingByteCorrect() {
    // Given: BlockStore with blocks (2,3), (5,10), (12,15)
    final String X_TIMES_16 = "xxxxxxxxxxxxxxxx";
    FakeObjectClient fakeObjectClient = new FakeObjectClient(X_TIMES_16);
    MetadataStore metadataStore =
        new MetadataStore(fakeObjectClient, PhysicalIOConfiguration.DEFAULT);
    BlockStore blockStore = new BlockStore(TEST_URI, metadataStore);

    blockStore.add(new Block(TEST_URI, fakeObjectClient, 2, 3, 0));
    blockStore.add(new Block(TEST_URI, fakeObjectClient, 5, 10, 0));
    blockStore.add(new Block(TEST_URI, fakeObjectClient, 12, 15, 0));

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

    blockStore.add(new Block(TEST_URI, fakeObjectClient, 2, 3, 0));
    blockStore.add(new Block(TEST_URI, fakeObjectClient, 5, 10, 0));
    blockStore.add(new Block(TEST_URI, fakeObjectClient, 12, 15, 0));

    // When & Then: we query for the next available byte, the result is correct
    assertEquals(OptionalLong.of(2), blockStore.findNextAvailableByte(0));
    assertEquals(OptionalLong.of(2), blockStore.findNextAvailableByte(1));
    assertEquals(OptionalLong.of(2), blockStore.findNextAvailableByte(2));
    assertEquals(OptionalLong.of(3), blockStore.findNextAvailableByte(3));
    assertEquals(OptionalLong.of(5), blockStore.findNextAvailableByte(4));
    assertEquals(OptionalLong.of(5), blockStore.findNextAvailableByte(5));
    assertEquals(OptionalLong.of(12), blockStore.findNextAvailableByte(11));
    assertEquals(OptionalLong.of(15), blockStore.findNextAvailableByte(15));
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
}
