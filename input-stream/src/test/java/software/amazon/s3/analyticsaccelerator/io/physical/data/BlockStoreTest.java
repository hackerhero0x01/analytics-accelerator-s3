/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.io.physical.data;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.*;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.*;

@SuppressWarnings("unchecked")
public class BlockStoreTest {

  private static final S3URI TEST_URI = S3URI.of("foo", "bar");
  private static final String ETAG = "RANDOM";
  private static final ObjectKey objectKey = ObjectKey.builder().s3URI(TEST_URI).etag(ETAG).build();
  private static final int OBJECT_SIZE = 100;
  private static final long DEFAULT_READ_TIMEOUT = 120_000;
  private static final int DEFAULT_READ_RETRY_COUNT = 20;

  @SneakyThrows
  @Test
  public void test__blockStore__getBlockAfterAddBlock() {
    // Given: empty BlockStore
    BlockStore blockStore =
        new BlockStore(
            mock(BlobStoreIndexCache.class), mock(Metrics.class), PhysicalIOConfiguration.DEFAULT);

    BlockKey blockKey = new BlockKey(objectKey, new Range(3, 5));

    // When: a new block is added
    blockStore.add(
        new Block(
            blockKey,
            0,
            mock(BlobStoreIndexCache.class),
            mock(Metrics.class),
            DEFAULT_READ_TIMEOUT));

    // Then: getBlock can retrieve the same block
    Optional<Block> b = blockStore.getBlock(4);

    assertTrue(b.isPresent());
    assertEquals(b.get().getBlockKey().getRange().getStart(), 3);
    assertEquals(b.get().getBlockKey().getRange().getEnd(), 5);
    assertEquals(b.get().getGeneration(), 0);
  }

  @Test
  public void test__blockStore__closesBlocks() throws IOException {
    // Given: BlockStore with a block
    BlockStore blockStore =
        new BlockStore(
            mock(BlobStoreIndexCache.class), mock(Metrics.class), PhysicalIOConfiguration.DEFAULT);
    Block block = mock(Block.class);
    blockStore.add(block);

    // When: blockStore is closed
    blockStore.close();

    // Then: underlying block is also closed
    verify(block, times(1)).close();
  }

  @Test
  public void test__blockStore__closeWorksWithExceptions() throws IOException {
    // Given: BlockStore with two blocks
    BlockStore blockStore =
        new BlockStore(
            mock(BlobStoreIndexCache.class), mock(Metrics.class), PhysicalIOConfiguration.DEFAULT);
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
