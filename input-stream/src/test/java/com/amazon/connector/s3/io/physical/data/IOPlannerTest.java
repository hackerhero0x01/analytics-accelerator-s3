package com.amazon.connector.s3.io.physical.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.ReadMode;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class IOPlannerTest {

  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  @SneakyThrows
  public void test__planRead__noopWhenBlockStoreEmpty() {
    // Given: an empty BlockStore
    final int OBJECT_SIZE = 10_000;
    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.headObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectMetadata.builder().contentLength(OBJECT_SIZE).build()));
    MetadataStore mockMetadataStore =
        new MetadataStore(objectClient, PhysicalIOConfiguration.DEFAULT);
    BlockStore blockStore = new BlockStore(TEST_URI, mockMetadataStore);
    IOPlanner ioPlanner = new IOPlanner(blockStore);

    // When: a read plan is requested for a range
    List<Range> missingRanges = ioPlanner.planRead(10, 100, OBJECT_SIZE - 1);

    // Then: it just falls through
    List<Range> expected = new LinkedList<>();
    expected.add(new Range(10, 100));

    assertEquals(expected, missingRanges);
  }

  @Test
  @SneakyThrows
  public void test__planRead__doesNotDoubleRead() {
    // Given: a BlockStore with a (100,200) block in it
    final int OBJECT_SIZE = 10_000;
    byte[] content = new byte[OBJECT_SIZE];
    MetadataStore metadataStore = getTestMetadataStoreWithContentLength(OBJECT_SIZE);
    BlockStore blockStore = new BlockStore(TEST_URI, metadataStore);
    FakeObjectClient fakeObjectClient = new FakeObjectClient(new String(content));
    blockStore.add(
        new Block(
            TEST_URI,
            fakeObjectClient,
            100,
            200,
            0,
            ReadMode.SYNC,
            PhysicalIOConfiguration.DEFAULT));
    IOPlanner ioPlanner = new IOPlanner(blockStore);

    // When: a read plan is requested for a range (0, 400)
    List<Range> missingRanges = ioPlanner.planRead(0, 400, OBJECT_SIZE - 1);

    // Then: we only request (0, 99) and (201, 400)
    List<Range> expected = new LinkedList<>();
    expected.add(new Range(0, 99));
    expected.add(new Range(201, 400));

    assertEquals(expected, missingRanges);
  }

  @Test
  @SneakyThrows
  public void test__planRead__regression_singleByteObject() {
    // Given: a single byte object and an empty block store
    final int OBJECT_SIZE = 1;
    MetadataStore metadataStore = getTestMetadataStoreWithContentLength(OBJECT_SIZE);
    BlockStore blockStore = new BlockStore(TEST_URI, metadataStore);
    IOPlanner ioPlanner = new IOPlanner(blockStore);

    // When: a read plan is requested for a range (0, 400)
    List<Range> missingRanges = ioPlanner.planRead(0, 400, OBJECT_SIZE - 1);

    // Then: we request the single byte range (0, 0)
    List<Range> expected = new LinkedList<>();
    expected.add(new Range(0, 0));

    assertEquals(expected, missingRanges);
  }

  private MetadataStore getTestMetadataStoreWithContentLength(long contentLength) {
    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.headObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectMetadata.builder().contentLength(contentLength).build()));

    return new MetadataStore(objectClient, PhysicalIOConfiguration.DEFAULT);
  }
}
