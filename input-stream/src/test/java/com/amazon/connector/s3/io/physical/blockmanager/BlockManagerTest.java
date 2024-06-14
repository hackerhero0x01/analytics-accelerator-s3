package com.amazon.connector.s3.io.physical.blockmanager;

import static com.amazon.connector.s3.util.Constants.ONE_MB;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.io.logical.ObjectStatus;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.utils.StringUtils;

public class BlockManagerTest {

  public static final String bucket = "bucket";
  public static final String key = "key";
  private static final S3URI URI = S3URI.of(bucket, key);

  @Test
  void testConstructor() {
    // When: constructor is called
    BlockManager blockManager =
        new BlockManager(mock(ObjectClient.class), URI, BlockManagerConfiguration.DEFAULT);

    // Then: result is not null
    assertNotNull(blockManager);
  }

  @Test
  void testConstructorFailsOnNull() {
    assertThrows(
        NullPointerException.class,
        () -> new BlockManager(null, URI, BlockManagerConfiguration.DEFAULT));
    assertThrows(
        NullPointerException.class,
        () ->
            new BlockManager(
                mock(ObjectClient.class), (S3URI) null, BlockManagerConfiguration.DEFAULT));
    assertThrows(
        NullPointerException.class, () -> new BlockManager(mock(ObjectClient.class), URI, null));
  }

  @Test
  void testDependentConstructor() {
    // When: constructor is called
    BlockManager blockManager = new BlockManager(mock(MultiObjectsBlockManager.class), URI);

    // Then: result is not null
    assertNotNull(blockManager);
  }

  @Test
  void testDependentConstructorFailsOnNull() {
    assertThrows(NullPointerException.class, () -> new BlockManager(null, URI));
    assertThrows(
        NullPointerException.class,
        () -> new BlockManager(mock(MultiObjectsBlockManager.class), (S3URI) null));
  }

  @Test
  void testClose() throws IOException {
    // Given: object client and block manager

    int contentLength = ONE_MB;
    byte[] content = new byte[contentLength];
    ObjectClient objectClient = mock(ObjectClient.class);
    BlockManager blockManager =
        new BlockManager(objectClient, URI, BlockManagerConfiguration.DEFAULT);

    when(objectClient.getObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ByteArrayInputStream(content)).build()));

    ObjectStatus objectStatus = mock(ObjectStatus.class);
    when(objectStatus.getS3URI()).thenReturn(URI);

    // When: the prefetch cache is not empty and close is called
    List<Range> prefetchRanges = new ArrayList<>();
    prefetchRanges.add(new Range(0, 100));
    blockManager.queuePrefetch(prefetchRanges);
    blockManager.close();

    // Object client is not closed, as we want to share the client b/w streams.
    verify(objectClient, times(0)).close();
  }

  @Test
  void testBlockManagerUsesMetadata() throws IOException {
    // Given: block manager
    int contentLength = ONE_MB;
    byte[] content = new byte[contentLength];

    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.getObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ByteArrayInputStream(content)).build()));
    when(objectClient.headObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectMetadata.builder().contentLength(contentLength).build()));
    BlockManager blockManager =
        new BlockManager(objectClient, URI, BlockManagerConfiguration.DEFAULT);

    // When: data asked for is more than object size
    byte[] buf = new byte[16 * ONE_MB];
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    blockManager.read(buf, 0, buf.length, 0);

    // Then: only object size is requested
    verify(objectClient).getObject(requestCaptor.capture());
    GetRequest getRequest = requestCaptor.getValue();
    assertEquals(0L, getRequest.getRange().getStart());
    assertEquals(ONE_MB - 1, getRequest.getRange().getEnd());
  }

  @Test
  void testBlockManagerGetMetadata() throws IOException {
    int contentLength = ONE_MB;

    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.headObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectMetadata.builder().contentLength(contentLength).build()));
    MultiObjectsBlockManager multiObjectsBlockManager =
        new MultiObjectsBlockManager(objectClient, BlockManagerConfiguration.DEFAULT);
    BlockManager blockManager = new BlockManager(multiObjectsBlockManager, URI);
    ObjectMetadata metadata = blockManager.getMetadata().join();

    ArgumentCaptor<HeadRequest> headRequestCaptor = ArgumentCaptor.forClass(HeadRequest.class);
    verify(objectClient).headObject(headRequestCaptor.capture());
    assertEquals(metadata.getContentLength(), contentLength);
  }

  @Test
  void testBlockManagerUsesReadAheadConfig() throws IOException {
    // Given: block manager
    int contentLength = ONE_MB;
    int readAheadConfig = 123;
    byte[] content = new byte[contentLength];

    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.getObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ByteArrayInputStream(content)).build()));
    when(objectClient.headObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectMetadata.builder().contentLength(contentLength).build()));
    BlockManager blockManager =
        new BlockManager(
            objectClient,
            URI,
            BlockManagerConfiguration.builder().readAheadBytes(readAheadConfig).build());

    // When: data asked for is less than read ahead config
    byte[] buf = new byte[16];
    ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
    blockManager.read(buf, 0, buf.length, 0);

    // Then: only object size is requested
    verify(objectClient).getObject(requestCaptor.capture());
    GetRequest getRequest = requestCaptor.getValue();
    assertEquals(0L, getRequest.getRange().getStart());
    assertEquals(readAheadConfig - 1, getRequest.getRange().getEnd());
  }

  @Test
  void testBlockManagerOneByteRead() throws IOException {
    // Given: block manager
    StringBuilder sb = new StringBuilder(10);
    sb.append(StringUtils.repeat("1", 10));
    FakeObjectClient objectClient = new FakeObjectClient(sb.toString());

    BlockManager blockManager =
        new BlockManager(objectClient, URI, BlockManagerConfiguration.DEFAULT);

    // When: called 1 byte read
    int byteRead = blockManager.read(1);

    // Then:  compare the byte read
    final byte expectedValue = sb.toString().getBytes(StandardCharsets.UTF_8)[1];
    assertEquals(byteRead, expectedValue);
  }

  @Test
  void testBlockManagerQueuePrefetch() throws IOException {
    int firstRangeStart = 0;
    int firstRangeEnd = 50;
    int secondRangeStart = 101;
    int secondRangeEnd = 200;
    StringBuilder sb = new StringBuilder(300);
    sb.append(StringUtils.repeat("0", 300));
    String str1 = StringUtils.repeat("1", firstRangeEnd - firstRangeStart + 1);
    String str2 = StringUtils.repeat("1", secondRangeEnd - secondRangeStart + 1);
    sb.replace(firstRangeStart, firstRangeEnd, str1);
    sb.replace(secondRangeStart, secondRangeEnd, str2);
    FakeObjectClient objectClient = new FakeObjectClient(sb.toString());

    BlockManager blockManager =
        new BlockManager(objectClient, URI, BlockManagerConfiguration.DEFAULT);
    List<Range> prefetchRanges = new ArrayList<>();
    prefetchRanges.add(new Range(secondRangeStart, secondRangeEnd));
    prefetchRanges.add(new Range(firstRangeStart, firstRangeEnd));
    ObjectStatus objectStatus = mock(ObjectStatus.class);
    when(objectStatus.getS3URI()).thenReturn(URI);

    blockManager.queuePrefetch(prefetchRanges);
    byte[] buf = new byte[firstRangeEnd - firstRangeStart + 1];
    blockManager.read(buf, 0, str1.length(), 0);
    assertArrayEquals(str1.getBytes(), buf);
    buf = new byte[secondRangeEnd - secondRangeStart + 1];
    blockManager.read(buf, 0, str2.length(), secondRangeStart);
    assertArrayEquals(str2.getBytes(), buf);
    assertEquals(2, objectClient.getGetRequestCount());
    assertEquals(1, objectClient.getHeadRequestCount());
  }

  @Test
  void testPrefetchError() throws IOException {
    // Given: Block Manager and creation of IOBlock throws Runtime exception when prefetch happens
    int contentLength = ONE_MB;
    byte[] content = new byte[contentLength];
    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.getObject(any()))
        .thenThrow(new RuntimeException("Throwing runtime exception"))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ByteArrayInputStream(content)).build()));
    when(objectClient.headObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectMetadata.builder().contentLength(contentLength).build()));
    BlockManager blockManager =
        new BlockManager(objectClient, URI, BlockManagerConfiguration.DEFAULT);

    // When: Prefetch is called
    List<Range> prefetchRanges = new ArrayList<>();
    prefetchRanges.add(new Range(0, 100));
    blockManager.queuePrefetch(prefetchRanges);

    // Then : Read data and should be fetched synchronously
    byte[] buf = new byte[16];
    blockManager.read(buf, 0, buf.length, 0);

    verify(objectClient, times(2)).getObject(any());
  }

  @Test
  void testBlockManagerDoNotPrefetchIfAlreadyPresent() throws IOException {
    // Given: Range from 0-100 is prefetched
    int prefetchStart = 0;
    int prefetchEnd = 100;

    StringBuilder sb = new StringBuilder(300);
    sb.append(StringUtils.repeat("0", 300));
    String str1 = StringUtils.repeat("1", prefetchEnd - prefetchStart + 1);
    sb.replace(prefetchStart, prefetchEnd, str1);

    FakeObjectClient objectClient = new FakeObjectClient(sb.toString());

    BlockManager blockManager =
        new BlockManager(objectClient, URI, BlockManagerConfiguration.DEFAULT);
    List<Range> prefetchRanges = new ArrayList<>();
    prefetchRanges.add(new Range(prefetchStart, prefetchEnd));

    ObjectStatus objectStatus = mock(ObjectStatus.class);
    when(objectStatus.getS3URI()).thenReturn(URI);

    blockManager.queuePrefetch(prefetchRanges);

    byte[] buf = new byte[prefetchEnd - prefetchStart + 1];
    blockManager.read(buf, 0, str1.length(), 0);
    assertArrayEquals(str1.getBytes(), buf);

    // When: Read for a Range already existing in the prefetch cache is called
    int secondRangeStart = 10;
    int secondRangeEnd = 50;
    prefetchRanges = new ArrayList<>();
    prefetchRanges.add(new Range(secondRangeStart, secondRangeEnd));
    blockManager.queuePrefetch(prefetchRanges);

    buf = new byte[secondRangeEnd - secondRangeStart + 1];
    blockManager.read(buf, 0, secondRangeEnd - secondRangeStart + 1, secondRangeStart);
    assertArrayEquals(str1.substring(secondRangeStart, secondRangeEnd + 1).getBytes(), buf);

    // Then: Ensure GET is called only once
    assertEquals(1, objectClient.getGetRequestCount());
    assertEquals(1, objectClient.getHeadRequestCount());
  }

  @Test
  void testBlockManagerPrefetchIfPartOfRangeIsPrefetched() throws IOException {
    // Given: Range 0->100 is prefetched and range 80 -> 150
    // (partly overlapping with prefetched range) is called

    int prefetchStart = 0;
    int prefetchEnd = 100;

    int secondRangeStart = 80;
    int secondRangeEnd = 150;

    StringBuilder sb = new StringBuilder(300);
    sb.append(StringUtils.repeat("0", 300));
    String str1 = StringUtils.repeat("1", prefetchEnd - prefetchStart + 1);
    String str2 = StringUtils.repeat("1", secondRangeEnd - secondRangeStart + 1);
    sb.replace(prefetchStart, prefetchEnd, str1);
    sb.replace(prefetchEnd, secondRangeEnd, str2);

    FakeObjectClient objectClient = new FakeObjectClient(sb.toString());

    BlockManager blockManager =
        new BlockManager(objectClient, URI, BlockManagerConfiguration.DEFAULT);
    List<Range> prefetchRanges = new ArrayList<>();
    prefetchRanges.add(new Range(prefetchStart, prefetchEnd));
    prefetchRanges.add(new Range(secondRangeStart, secondRangeEnd));

    ObjectStatus objectStatus = mock(ObjectStatus.class);
    when(objectStatus.getS3URI()).thenReturn(URI);

    // Prefetch for range 0->100 and then from 80-> 150
    blockManager.queuePrefetch(prefetchRanges);

    byte[] buf = new byte[prefetchEnd - prefetchStart + 1];
    blockManager.read(buf, 0, str1.length(), 0);
    assertArrayEquals(str1.getBytes(), buf);

    buf = new byte[secondRangeEnd - secondRangeStart + 1];
    blockManager.read(buf, 0, secondRangeEnd - secondRangeStart + 1, secondRangeStart);
    assertArrayEquals(
        sb.toString().substring(secondRangeStart, secondRangeEnd + 1).getBytes(), buf);

    // Then: Issue two prefetch requests one from 0->100 and another from 100->150
    assertEquals(2, objectClient.getGetRequestCount());
    assertEquals(1, objectClient.getHeadRequestCount());
  }

  @Test
  void testPrefetchAddsToOnlyPrefetchCache() throws IOException {
    // Given: MultiObjectBlock Manager
    Map<S3URI, CompletableFuture<ObjectMetadata>> metadata = new LinkedHashMap<>();
    Map<S3URI, AutoClosingCircularBuffer<IOBlock>> ioBlocks = new LinkedHashMap<>();
    Map<S3URI, AutoClosingCircularBuffer<PrefetchIOBlock>> prefetchCache = new LinkedHashMap<>();

    StringBuilder sb = new StringBuilder(300);
    sb.append(StringUtils.repeat("1", 300));
    FakeObjectClient objectClient = new FakeObjectClient(sb.toString());

    MultiObjectsBlockManager multiObjectsBlockManager =
        new MultiObjectsBlockManager(
            objectClient, BlockManagerConfiguration.DEFAULT, metadata, ioBlocks, prefetchCache);
    BlockManager blockManager = new BlockManager(multiObjectsBlockManager, URI);

    // When: Queue prefetch
    List<Range> prefetchRanges = new ArrayList<>();
    prefetchRanges.add(new Range(150, 200));
    blockManager.queuePrefetch(prefetchRanges);

    // Then: Check that IOBlock is only added to the prefetch cache
    Optional<PrefetchIOBlock> prefetchIOBlock =
        prefetchCache.get(URI).stream().filter(block -> block.contains(160)).findFirst();
    assertTrue(prefetchIOBlock.isPresent());

    Optional<IOBlock> ioBlock =
        ioBlocks.get(URI).stream().filter(block -> block.contains(160)).findFirst();
    assertFalse(ioBlock.isPresent());

    // When: Read is called on range that is not prefetched
    byte[] buf = new byte[100];
    blockManager.read(buf, 0, 100, 0);

    // Then: IOBlock is only present in the ioBlocks cache
    ioBlock = ioBlocks.get(URI).stream().filter(block -> block.contains(90)).findFirst();
    assertTrue(ioBlock.isPresent());

    prefetchIOBlock =
        prefetchCache.get(URI).stream().filter(block -> block.contains(90)).findFirst();
    assertTrue(!prefetchIOBlock.isPresent());

    // When: Subsequent read is called for prefetched range
    buf = new byte[50];
    blockManager.read(buf, 0, 50, 150);

    blockManager.read(buf, 0, 50, 10);

    // Then: IOBlocks are served from the appropriate caches and new blocks are not created
    assertTrue(prefetchCache.get(URI).stream().count() == 1);
    assertTrue(ioBlocks.get(URI).stream().count() == 1);
  }

  @Test
  void testBlockManagerPrefetchErrorDoesNotThrow() throws IOException {
    // Given: block manager AND S3 is unavailable
    int contentLength = ONE_MB;
    int readAheadConfig = 123;

    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.getObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ThrowingInputStream()).build()));
    when(objectClient.headObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectMetadata.builder().contentLength(contentLength).build()));

    // When: Prefetching
    int prefetchStart = 0;
    int prefetchEnd = 100;

    List<Range> prefetchRanges = new ArrayList<>();
    prefetchRanges.add(new Range(prefetchStart, prefetchEnd));

    BlockManager blockManager =
        new BlockManager(
            objectClient,
            URI,
            BlockManagerConfiguration.builder().readAheadBytes(readAheadConfig).build());

    // Then: Do not throw error for prefetching. Reading should throw error
    blockManager.queuePrefetch(prefetchRanges);

    byte[] buf = new byte[16];
    assertThrows(IOException.class, () -> blockManager.read(buf, 0, buf.length, 0));
  }

  @Test
  // TODO: This test should be modified at some point to test for retries when we introduce them:
  //  ticket: https://app.asana.com/0/1206885953994785/1207374694729991/f
  void testBlockManagerDoesNotRetry() throws IOException {
    // Given: block manager AND S3 is unavailable
    int contentLength = ONE_MB;
    int readAheadConfig = 123;

    ObjectClient objectClient = mock(ObjectClient.class);
    when(objectClient.getObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectContent.builder().stream(new ThrowingInputStream()).build()));
    when(objectClient.headObject(any()))
        .thenReturn(
            CompletableFuture.completedFuture(
                ObjectMetadata.builder().contentLength(contentLength).build()));
    BlockManager blockManager =
        new BlockManager(
            objectClient,
            URI,
            BlockManagerConfiguration.builder().readAheadBytes(readAheadConfig).build());

    // Then: block manager throws IOException
    byte[] buf = new byte[16];
    assertThrows(IOException.class, () -> blockManager.read(buf, 0, buf.length, 0));
  }

  private static class ThrowingInputStream extends InputStream {
    @Override
    public int read() throws IOException {
      throw new IOException("I always throw!");
    }
  }
}
