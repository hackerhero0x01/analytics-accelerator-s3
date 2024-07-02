package com.amazon.connector.s3.io.physical.blockmanager;

import static com.amazon.connector.s3.util.Constants.ONE_MB;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.utils.StringUtils;

public class MultiObjectsBlockManagerTest {
  @Test
  public void testDefaultConstructor() {
    MultiObjectsBlockManager multiObjectsBlockManager =
        new MultiObjectsBlockManager(mock(ObjectClient.class), BlockManagerConfiguration.DEFAULT);
    assertNotNull(multiObjectsBlockManager);

    multiObjectsBlockManager =
        new MultiObjectsBlockManager(
            mock(ObjectClient.class),
            BlockManagerConfiguration.builder()
                .capacityMultiObjects(10)
                .capacityPrefetchCache(10)
                .capacityBlocks(17)
                .build());
    assertNotNull(multiObjectsBlockManager);
  }

  @Test
  public void testConstructorWithNullParams() {
    assertThrows(
        NullPointerException.class,
        () -> new MultiObjectsBlockManager(null, BlockManagerConfiguration.DEFAULT));
    assertThrows(
        NullPointerException.class,
        () -> new MultiObjectsBlockManager(mock(ObjectClient.class), null));
  }

  @Test
  public void testCaches() throws IOException, InterruptedException {
    StringBuilder sb = new StringBuilder(8 * ONE_MB);
    sb.append(StringUtils.repeat("0", 8 * ONE_MB));
    FakeObjectClient objectClient = new FakeObjectClient(sb.toString());

    MultiObjectsBlockManager multiObjectsBlockManager =
        new MultiObjectsBlockManager(
            objectClient,
            BlockManagerConfiguration.builder()
                .capacityMultiObjects(1)
                .capacityPrefetchCache(1)
                .capacityBlocks(1)
                .build());
    S3URI s3URI1 = S3URI.of("test", "test1");
    S3URI s3URI2 = S3URI.of("test", "test2");
    ArrayList<Range> ranges =
        new ArrayList() {
          {
            add(new Range(0, ONE_MB));
            add(new Range(ONE_MB + 1, 2 * ONE_MB));
          }
        };

    List<CompletableFuture<IOBlock>> prefetchResults =
        multiObjectsBlockManager.queuePrefetch(ranges, s3URI1);
    prefetchResults.forEach(prefetchBlock -> prefetchBlock.join());

    multiObjectsBlockManager.read(4 * ONE_MB, s3URI1);
    multiObjectsBlockManager.read(3 * ONE_MB, s3URI1);

    prefetchResults = multiObjectsBlockManager.queuePrefetch(ranges, s3URI2);
    prefetchResults.forEach(prefetchBlock -> prefetchBlock.join());

    multiObjectsBlockManager.read(4 * ONE_MB, s3URI2);
    multiObjectsBlockManager.read(3 * ONE_MB, s3URI2);

    assertEquals(2, objectClient.getHeadRequestCount().get());
    assertEquals(8, objectClient.getGetRequestCount().get());
  }

  @Test
  void testPrefetchWithIncorrectRanges() throws InterruptedException {
    StringBuilder sb = new StringBuilder(8 * ONE_MB);
    sb.append(StringUtils.repeat("0", 8 * ONE_MB));
    FakeObjectClient objectClient = new FakeObjectClient(sb.toString());
    MultiObjectsBlockManager multiObjectsBlockManager =
        new MultiObjectsBlockManager(objectClient, BlockManagerConfiguration.DEFAULT);
    S3URI s3URI = S3URI.of("test", "test");

    multiObjectsBlockManager.queuePrefetch(Arrays.asList(new Range(100, 90)), s3URI);
    multiObjectsBlockManager.queuePrefetch(
        Arrays.asList(new Range(9 * ONE_MB, 10 * ONE_MB)), s3URI);
    assertEquals(0, objectClient.getGetRequestCount().get());
    // that will prefetch 0-0
    List<CompletableFuture<IOBlock>> prefetchResults =
        multiObjectsBlockManager.queuePrefetch(Arrays.asList(new Range(0, -10)), s3URI);
    prefetchResults.forEach(prefetchBlock -> prefetchBlock.join());
    // that will try to look for 0-10, but will find 0-0 and do nothing
    prefetchResults =
        multiObjectsBlockManager.queuePrefetch(Arrays.asList(new Range(0, 10)), s3URI);
    prefetchResults.forEach(prefetchBlock -> prefetchBlock.join());

    prefetchResults =
        multiObjectsBlockManager.queuePrefetch(Arrays.asList(new Range(-10, 10)), s3URI);
    prefetchResults.forEach(prefetchBlock -> prefetchBlock.join());

    assertEquals(1, objectClient.getRequestedRanges().size());
    assertEquals(0, objectClient.getRequestedRanges().getFirst().getStart().orElse(-1));
    assertEquals(0, objectClient.getRequestedRanges().getFirst().getEnd().orElse(-1));
  }

  @Test
  void testAbsenceOfDuplicateReading() throws IOException {
    StringBuilder sb = new StringBuilder(8 * ONE_MB);
    sb.append(StringUtils.repeat("0", 8 * ONE_MB));
    FakeObjectClient objectClient = new FakeObjectClient(sb.toString());
    MultiObjectsBlockManager multiObjectsBlockManager =
        new MultiObjectsBlockManager(objectClient, BlockManagerConfiguration.DEFAULT);
    S3URI s3URI = S3URI.of("test", "test");

    byte[] buffer = new byte[ONE_MB];
    multiObjectsBlockManager.read(buffer, 0, 1, 0, s3URI);
    multiObjectsBlockManager.read(buffer, 0, 1, 10, s3URI);
    // with first read we should read up to READ_AHEAD bytes with one get request

    multiObjectsBlockManager.read(
        buffer, 0, (int) BlockManagerConfiguration.DEFAULT_READ_AHEAD_BYTES, 100, s3URI);
    // this read should initiate second get request outside the first one
    assertEquals(2, objectClient.getGetRequestCount().get());

    assertEquals(2, objectClient.getRequestedRanges().size());
    // ensure that Get ranges is not overlapping
    assertEquals(0, objectClient.getRequestedRanges().getFirst().getStart().orElse(-1));
    assertEquals(
        BlockManagerConfiguration.DEFAULT_READ_AHEAD_BYTES - 1,
        objectClient.getRequestedRanges().getFirst().getEnd().orElse(-1));
    assertEquals(
        BlockManagerConfiguration.DEFAULT_READ_AHEAD_BYTES,
        objectClient.getRequestedRanges().getLast().getStart().orElse(-1));
    assertEquals(
        BlockManagerConfiguration.DEFAULT_READ_AHEAD_BYTES * 2 - 1,
        objectClient.getRequestedRanges().getLast().getEnd().orElse(-1));

    multiObjectsBlockManager.read(
        buffer, 0, (int) BlockManagerConfiguration.DEFAULT_READ_AHEAD_BYTES, 1000, s3URI);
    // previous two GET request should be enough for providing data for this read request
    assertEquals(2, objectClient.getGetRequestCount().get());

    multiObjectsBlockManager.read(
        buffer, 0, (int) BlockManagerConfiguration.DEFAULT_READ_AHEAD_BYTES * 3, 1000, s3URI);
    // with this read we should use two cached Get request and create new Get request bugger than
    // READ_AHEAD
    assertEquals(3, objectClient.getGetRequestCount().get());
    assertEquals(
        BlockManagerConfiguration.DEFAULT_READ_AHEAD_BYTES * 2,
        objectClient.getRequestedRanges().getLast().getStart().orElse(-1));
    assertEquals(
        BlockManagerConfiguration.DEFAULT_READ_AHEAD_BYTES * 3 + 999,
        objectClient.getRequestedRanges().getLast().getEnd().orElse(-1));
  }

  @Test
  void testTailRead() throws IOException {
    StringBuilder sb = new StringBuilder(4 * ONE_MB);
    final int HALF_MB = ONE_MB / 2;
    sb.append(StringUtils.repeat("0", 3 * ONE_MB + HALF_MB));
    sb.append(StringUtils.repeat("1", HALF_MB));
    FakeObjectClient objectClient = new FakeObjectClient(sb.toString());
    MultiObjectsBlockManager multiObjectsBlockManager =
        new MultiObjectsBlockManager(objectClient, BlockManagerConfiguration.DEFAULT);
    S3URI s3URI = S3URI.of("test", "test");

    byte[] buffer = new byte[HALF_MB];
    byte[] expectedBuffer = new byte[HALF_MB];
    Arrays.fill(expectedBuffer, 0, HALF_MB, (byte) '1');
    multiObjectsBlockManager.readTail(buffer, 0, HALF_MB, s3URI);
    assertArrayEquals(expectedBuffer, buffer);

    buffer = new byte[ONE_MB];
    expectedBuffer = new byte[ONE_MB];
    Arrays.fill(expectedBuffer, 0, HALF_MB, (byte) '0');
    Arrays.fill(expectedBuffer, HALF_MB, ONE_MB, (byte) '1');
    multiObjectsBlockManager.readTail(buffer, 0, ONE_MB, s3URI);
    assertArrayEquals(expectedBuffer, buffer);
  }

  @Test
  public void testGetMetadataWithFailedRequest() throws Exception {
    ObjectClient objectClient = mock(ObjectClient.class);
    MultiObjectsBlockManager multiObjectsBlockManager =
        new MultiObjectsBlockManager(objectClient, BlockManagerConfiguration.DEFAULT);
    when(objectClient.headObject(any()))
        .thenAnswer(
            new Answer<CompletableFuture<ObjectMetadata>>() {
              private int count = 0;

              public CompletableFuture<ObjectMetadata> answer(
                  org.mockito.invocation.InvocationOnMock invocation) throws Throwable {
                count++;
                if (count != 1)
                  return CompletableFuture.supplyAsync(
                      () -> ObjectMetadata.builder().contentLength(10).build());

                CompletableFuture<ObjectMetadata> future = new CompletableFuture<>();
                future.completeExceptionally(new Exception("test"));
                return future;
              }
            });

    S3URI s3URI = S3URI.of("test", "test");
    multiObjectsBlockManager.getMetadata(s3URI);
    multiObjectsBlockManager.getMetadata(s3URI);
    verify(objectClient, times(2)).headObject(any());
  }

  @Test
  public void testGetMetadata() throws Exception {
    ObjectClient objectClient = mock(ObjectClient.class);
    MultiObjectsBlockManager multiObjectsBlockManager =
        new MultiObjectsBlockManager(objectClient, BlockManagerConfiguration.DEFAULT);
    when(objectClient.headObject(any()))
        .thenReturn(
            CompletableFuture.supplyAsync(
                () -> ObjectMetadata.builder().contentLength(10).build()));
    S3URI s3URI = S3URI.of("test", "test");
    multiObjectsBlockManager.getMetadata(s3URI);
    multiObjectsBlockManager.getMetadata(s3URI);
    verify(objectClient, times(1)).headObject(any());
  }

  @Test
  public void testGetMetadataWithTwoDifferentKeys() throws Exception {
    ObjectClient objectClient = mock(ObjectClient.class);
    MultiObjectsBlockManager multiObjectsBlockManager =
        new MultiObjectsBlockManager(objectClient, BlockManagerConfiguration.DEFAULT);
    when(objectClient.headObject(any()))
        .thenReturn(
            CompletableFuture.supplyAsync(
                () -> ObjectMetadata.builder().contentLength(10).build()));
    S3URI s3URI1 = S3URI.of("test", "test1");
    S3URI s3URI2 = S3URI.of("test", "test2");
    multiObjectsBlockManager.getMetadata(s3URI1);
    multiObjectsBlockManager.getMetadata(s3URI2);
    verify(objectClient, times(2)).headObject(any());
  }

  @Test
  public void testClose() throws Exception {
    ObjectClient objectClient = mock(ObjectClient.class);
    MultiObjectsBlockManager multiObjectsBlockManager =
        new MultiObjectsBlockManager(objectClient, BlockManagerConfiguration.DEFAULT);
    assertDoesNotThrow(() -> multiObjectsBlockManager.close());
  }
}
