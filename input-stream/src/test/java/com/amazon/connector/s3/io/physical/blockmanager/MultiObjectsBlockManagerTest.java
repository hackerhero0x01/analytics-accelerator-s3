package com.amazon.connector.s3.io.physical.blockmanager;

import static com.amazon.connector.s3.util.Constants.ONE_MB;
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
import com.amazon.connector.s3.io.logical.parquet.ColumnMappers;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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

    multiObjectsBlockManager.queuePrefetch(ranges, s3URI1);
    multiObjectsBlockManager.read(4 * ONE_MB, s3URI1);
    multiObjectsBlockManager.read(3 * ONE_MB, s3URI1);

    multiObjectsBlockManager.queuePrefetch(ranges, s3URI2);
    multiObjectsBlockManager.read(4 * ONE_MB, s3URI2);
    multiObjectsBlockManager.read(3 * ONE_MB, s3URI2);

    assertEquals(2, objectClient.getHeadRequestCount().get());
    assertEquals(8, objectClient.getGetRequestCount().get());
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
  public void testColumnMappers() {
    ObjectClient objectClient = mock(ObjectClient.class);
    Map<S3URI, CompletableFuture<ObjectMetadata>> metadata = new HashMap<>();
    Map<S3URI, AutoClosingCircularBuffer<IOBlock>> ioBlocks = new HashMap<>();
    Map<S3URI, AutoClosingCircularBuffer<PrefetchIOBlock>> prefetchCache = new HashMap<>();
    Map<S3URI, ColumnMappers> columnMappersStore = new HashMap<>();
    Map<String, String> recentColumns = new HashMap<>();

    MultiObjectsBlockManager multiObjectsBlockManager =
        new MultiObjectsBlockManager(
            objectClient,
            BlockManagerConfiguration.DEFAULT,
            metadata,
            ioBlocks,
            prefetchCache,
            columnMappersStore,
            recentColumns);

    S3URI s3URI = S3URI.of("test", "test1");
    ColumnMappers columnMappers = new ColumnMappers(new HashMap<>(), new HashMap<>());

    multiObjectsBlockManager.putColumnMappers(s3URI, columnMappers);
    assertEquals(columnMappers, multiObjectsBlockManager.getColumnMappers(s3URI));
  }

  @Test
  public void testClose() throws Exception {
    ObjectClient objectClient = mock(ObjectClient.class);
    MultiObjectsBlockManager multiObjectsBlockManager =
        new MultiObjectsBlockManager(objectClient, BlockManagerConfiguration.DEFAULT);
    assertDoesNotThrow(() -> multiObjectsBlockManager.close());
  }
}
