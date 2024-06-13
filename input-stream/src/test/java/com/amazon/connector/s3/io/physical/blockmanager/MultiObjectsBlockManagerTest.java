package com.amazon.connector.s3.io.physical.blockmanager;
import static com.amazon.connector.s3.util.Constants.ONE_KB;
import static com.amazon.connector.s3.util.Constants.ONE_MB;
import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.util.io.IOUtil.close;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.utils.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

public class MultiObjectsBlockManagerTest {
    @Test
    public void testDefaultConstructor() {
        MultiObjectsBlockManager multiObjectsBlockManager = new MultiObjectsBlockManager(mock(ObjectClient.class), BlockManagerConfiguration.DEFAULT);
        assertNotNull(multiObjectsBlockManager);

        multiObjectsBlockManager = new MultiObjectsBlockManager(mock(ObjectClient.class),
                                                                BlockManagerConfiguration.builder().
                                                                        capacityMultiObjects(10).
                                                                        capacityPrefetchCache(10).
                                                                        capacityBlocks(17).build());
        assertNotNull(multiObjectsBlockManager);
    }

    @Test
    public void testConstructorWithNullParams() {
        assertThrows(NullPointerException.class, () -> new MultiObjectsBlockManager(null, BlockManagerConfiguration.DEFAULT));
        assertThrows(NullPointerException.class, () -> new MultiObjectsBlockManager(mock(ObjectClient.class), null));
    }

    @Test
    public void testCaches() throws IOException {
        StringBuilder sb = new StringBuilder(8 * ONE_MB);
        sb.append(StringUtils.repeat("0", 8 * ONE_MB));
        FakeObjectClient objectClient = new FakeObjectClient(sb.toString());

        MultiObjectsBlockManager multiObjectsBlockManager = new MultiObjectsBlockManager(objectClient,
                                                                BlockManagerConfiguration.builder().
                                                                        capacityMultiObjects(1).
                                                                        capacityPrefetchCache(1).
                                                                        capacityBlocks(1).build());
        S3URI s3URI1 = S3URI.of("test", "test1");
        S3URI s3URI2 = S3URI.of("test", "test2");
        ArrayList<Range> ranges = new ArrayList() {
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

        assertEquals(2, objectClient.getHeadRequestCount());
        assertEquals(8, objectClient.getGetRequestCount());
    }

    @Test
    public void testGetMetadataWithFailedRequest() throws Exception {
        ObjectClient objectClient = mock(ObjectClient.class);
        MultiObjectsBlockManager multiObjectsBlockManager = new MultiObjectsBlockManager(objectClient, BlockManagerConfiguration.DEFAULT);
        when(objectClient.headObject(any())).
                thenAnswer(new Answer<CompletableFuture<ObjectMetadata>>() {
                    private int count = 0;
                    public CompletableFuture<ObjectMetadata> answer(org.mockito.invocation.InvocationOnMock invocation) throws Throwable{
                        count ++;
                        if (count != 1)
                            return  CompletableFuture.supplyAsync(() -> ObjectMetadata.builder().contentLength(10).build());

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
        MultiObjectsBlockManager multiObjectsBlockManager = new MultiObjectsBlockManager(objectClient, BlockManagerConfiguration.DEFAULT);
        when(objectClient.headObject(any())).thenReturn(CompletableFuture.supplyAsync(() -> ObjectMetadata.builder().contentLength(10).build()));
        S3URI s3URI = S3URI.of("test", "test");
        multiObjectsBlockManager.getMetadata(s3URI);
        multiObjectsBlockManager.getMetadata(s3URI);
        verify(objectClient, times(1)).headObject(any());
    }

    @Test
    public void testGetMetadataWithTwoDifferentKeys() throws Exception {
        ObjectClient objectClient = mock(ObjectClient.class);
        MultiObjectsBlockManager multiObjectsBlockManager = new MultiObjectsBlockManager(objectClient, BlockManagerConfiguration.DEFAULT);
        when(objectClient.headObject(any())).thenReturn(CompletableFuture.supplyAsync(() -> ObjectMetadata.builder().contentLength(10).build()));
        S3URI s3URI1 = S3URI.of("test", "test1");
        S3URI s3URI2 = S3URI.of("test", "test2");
        multiObjectsBlockManager.getMetadata(s3URI1);
        multiObjectsBlockManager.getMetadata(s3URI2);
        verify(objectClient, times(2)).headObject(any());
    }
/*
    @Test
    public void testQueuePrefetch() throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder(8 * ONE_MB);
        sb.append(StringUtils.repeat("0", 300));
        FakeObjectClient objectClient = new FakeObjectClient(sb.toString());
        MultiObjectsBlockManager multiObjectsBlockManager = new MultiObjectsBlockManager(objectClient,
                                                                                         BlockManagerConfiguration.DEFAULT);

        S3URI s3URI = S3URI.of("test", "test");
        ArrayList<Range> ranges = new ArrayList<Range>() {
            {
                add(new Range(0,  10));
                add(new Range(100, 110));
            }
        };
        multiObjectsBlockManager.queuePrefetch(ranges, s3URI);
        //sleep(2000);
        assertEquals(2, objectClient.getGetRequestCount());

        Range range1 = new Range(7, 20);
        multiObjectsBlockManager.queuePrefetch(new ArrayList<Range>() {{add(range1);}}, s3URI);
        sleep(2000);
        assertEquals(3, objectClient.getGetRequestCount());

        Range range2 = new Range(90, 103);
        multiObjectsBlockManager.queuePrefetch(new ArrayList<Range>() {{add(range2);}}, s3URI);
        sleep(2000);
        assertEquals(4, objectClient.getGetRequestCount());

        Range range3 = new Range(200, 210);
        multiObjectsBlockManager.queuePrefetch(new ArrayList<Range>() {{add(range3);}}, s3URI);
        sleep(2000);
        assertEquals(5, objectClient.getGetRequestCount());
    }
*/
}
