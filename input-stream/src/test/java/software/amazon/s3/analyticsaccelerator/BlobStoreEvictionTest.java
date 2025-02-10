package software.amazon.s3.analyticsaccelerator;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.data.Blob;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.io.physical.data.MemoryManager;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.List;


public class BlobStoreEvictionTest {

    @Mock
    private ObjectClient mockObjectClient;
    @Mock
    private Telemetry mockTelemetry;
    @Mock
    private PhysicalIOConfiguration mockConfiguration;
    @Mock
    private MemoryManager mockMemoryManager;

    private BlobStore blobStore;

    public void setUp() {
        MockitoAnnotations.openMocks(this);
        doReturn((long) 9 * ONE_MB).when(mockConfiguration).getMaxMemoryLimitAAL();
        BlobStore realBlobStore = new BlobStore(mockObjectClient, mockTelemetry, mockConfiguration);
        blobStore = spy(realBlobStore);
        doReturn(1024 * 1024L).when(blobStore).getBlobSize(any());
    }

    @Test
    void testBlobStoreMemoryLimitMaxMemoryLimitWhenConfigured() {
        // Given
        long configuredMaxMemory = 1000000L; // 1 MB
        PhysicalIOConfiguration config = mock(PhysicalIOConfiguration.class);
        when(config.getMaxMemoryLimitAAL()).thenReturn(configuredMaxMemory);

        // When
        BlobStore blobStore = new BlobStore(mock(ObjectClient.class), TestTelemetry.DEFAULT, config);

        // Then
        long expectedReserveMemory = Math.min((long) Math.ceil(0.10 * configuredMaxMemory), BlobStore.RESERVE_MEMORY);
        long expectedMaxMemoryLimit = configuredMaxMemory - expectedReserveMemory;
        assertEquals(expectedMaxMemoryLimit, blobStore.getBlobStoreMaxMemoryLimit());
    }

    @Test
    void testBlobStoreMemoryLimitMaxMemoryLimitWhenNotConfigured() {
        // Given
        PhysicalIOConfiguration config = mock(PhysicalIOConfiguration.class);
        when(config.getMaxMemoryLimitAAL()).thenReturn(Long.MAX_VALUE);

        // When
        BlobStore blobStore = new BlobStore(mock(ObjectClient.class), TestTelemetry.DEFAULT, config);

        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        long maxHeapMemory = memoryBean.getHeapMemoryUsage().getMax();
        long expectedLimit = maxHeapMemory / 2;

        // Then
        assertEquals(expectedLimit, blobStore.getBlobStoreMaxMemoryLimit());
    }

    @Test
    public void testEvictionTriggered() {
        setUp();

        int blobCount = 10;
        for (int i = 0; i < blobCount; i++) {
            ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
            ObjectMetadata metadata = mock(ObjectMetadata.class);
            when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
            StreamContext streamContext = mock(StreamContext.class);
            Blob blob = blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
            blobStore.updateMemoryUsage(1024 * 1024L);

        }

        // Verify that eviction was triggered
        assertTrue(blobStore.blobCount() < blobCount);
        assertTrue(blobStore.getMemoryUsage() < blobStore.getBlobStoreMaxMemoryLimit());
    }

    @Test
    public void testEvictionPreservesActiveReaders() {
        setUp();

        for (int i = 0; i < 5; i++) {
            String s3Uri = "s3://bucket/key" + i;
            ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
            ObjectMetadata metadata = mock(ObjectMetadata.class);
            when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
            StreamContext streamContext = mock(StreamContext.class);

            Blob blob = blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
            blob.updateActiveReaders(1); // Simulate active reader
            blobStore.updateMemoryUsage(1024 * 1024L);
        }

        // Add more blobs to trigger eviction
        for (int i = 5; i < 10; i++) {
            ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
            ObjectMetadata metadata = mock(ObjectMetadata.class);
            when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
            StreamContext streamContext = mock(StreamContext.class);

            Blob blob = blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
            blobStore.updateMemoryUsage(1024 * 1024L);
        }

        // Verify that blobs with active readers are not evicted
        assertEquals(7, blobStore.blobCount());
        for (int i = 0; i < 5; i++) {
            ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
            assertNotNull(blobStore.get(objectKey));
        }
    }

    @Test
    public void testEvictionOrder() {
        setUp();
        // Add blobs in a specific order
        for (int i = 0; i < 5; i++) {
            ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
            ObjectMetadata metadata = mock(ObjectMetadata.class);
            when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
            StreamContext streamContext = mock(StreamContext.class);

            Blob blob = blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
            blobStore.updateMemoryUsage(1024 * 1024L);
        }

        // Access blobs in reverse order
        for (int i = 4; i >= 0; i--) {
            ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
            blobStore.get(objectKey);
        }

        // Add more blobs to trigger eviction
        for (int i = 5; i < 8; i++) {
            ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
            ObjectMetadata metadata = mock(ObjectMetadata.class);
            when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
            StreamContext streamContext = mock(StreamContext.class);

            Blob blob = blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
            blobStore.updateMemoryUsage(1024 * 1024L);
        }

        // Verify that the least recently used blobs were evicted
        assertNotNull(blobStore.get(new ObjectKey(S3URI.of("bucket", "key0"), "etag0")));
        assertNotNull(blobStore.get(new ObjectKey(S3URI.of("bucket", "key1"), "etag1")));
        assertNull(blobStore.get(new ObjectKey(S3URI.of("bucket", "key2"), "etag2")));
        assertNull(blobStore.get(new ObjectKey(S3URI.of("bucket", "key3"), "etag3")));
        assertNull(blobStore.get(new ObjectKey(S3URI.of("bucket", "key4"), "etag4")));
    }

    @Test
    public void testAccessOrderingWithoutEviction() {
        setUp();
        // Set a larger memory limit to avoid eviction
        doReturn((long) 20 * ONE_MB).when(mockConfiguration).getMaxMemoryLimitAAL();

        // Add blobs in a specific order
        for (int i = 0; i < 5; i++) {
            ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
            ObjectMetadata metadata = mock(ObjectMetadata.class);
            when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
            StreamContext streamContext = mock(StreamContext.class);

            blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
            blobStore.updateMemoryUsage(1024 * 1024L);
        }

        // Access blobs in reverse order
        for (int i = 4; i >= 0; i--) {
            ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
            blobStore.get(objectKey);
        }

        // Verify the order by checking the internal order of the map
        List<ObjectKey> expectedOrder = new ArrayList<>();
        for (int i = 4; i >= 0; i--) {
            expectedOrder.add(new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i));
        }

        List<ObjectKey> actualOrder = blobStore.getKeyOrder();

        assertEquals(expectedOrder, actualOrder, "The access order of the blobs should match the expected order");
    }
}
