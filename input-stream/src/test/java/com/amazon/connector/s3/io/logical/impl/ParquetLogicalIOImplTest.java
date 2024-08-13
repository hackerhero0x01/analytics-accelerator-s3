package com.amazon.connector.s3.io.logical.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.io.physical.data.BlobStore;
import com.amazon.connector.s3.io.physical.data.MetadataStore;
import com.amazon.connector.s3.io.physical.impl.PhysicalIOImpl;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class ParquetLogicalIOImplTest {

  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  void testConstructor() {
    assertNotNull(
        new ParquetLogicalIOImpl(
            S3URI.of("foo", "bar"),
            mock(PhysicalIO.class),
            Telemetry.NOOP,
            mock(LogicalIOConfiguration.class),
            mock(ParquetMetadataStore.class)));
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                null,
                Telemetry.NOOP,
                mock(LogicalIOConfiguration.class),
                mock(ParquetMetadataStore.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                mock(PhysicalIO.class),
                Telemetry.NOOP,
                null,
                mock(ParquetMetadataStore.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                mock(PhysicalIO.class),
                null,
                mock(LogicalIOConfiguration.class),
                mock(ParquetMetadataStore.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                mock(PhysicalIO.class),
                Telemetry.NOOP,
                mock(LogicalIOConfiguration.class),
                null));
  }

  @Test
  void testCloseDependencies() throws IOException {
    // Given
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    LogicalIOConfiguration configuration =
        LogicalIOConfiguration.builder()
            .footerCachingEnabled(false)
            .metadataAwarePrefetchingEnabled(false)
            .build();

    ParquetLogicalIOImpl logicalIO =
        new ParquetLogicalIOImpl(
            TEST_URI,
            physicalIO,
            Telemetry.NOOP,
            configuration,
            new ParquetMetadataStore(configuration));

    // When: close called
    logicalIO.close();

    // Then: close will close dependencies
    verify(physicalIO, times(1)).close();
  }

  @Test
  void testMetadaWithZeroContentLength() {
    ObjectClient mockClient = mock(ObjectClient.class);
    when(mockClient.headObject(any(HeadRequest.class)))
        .thenReturn(
            CompletableFuture.completedFuture(ObjectMetadata.builder().contentLength(0).build()));
    S3URI s3URI = S3URI.of("test", "test");
    MetadataStore metadataStore =
        new MetadataStore(mockClient, Telemetry.NOOP, PhysicalIOConfiguration.DEFAULT);
    BlobStore blobStore =
        new BlobStore(metadataStore, mockClient, Telemetry.NOOP, PhysicalIOConfiguration.DEFAULT);
    PhysicalIOImpl physicalIO = new PhysicalIOImpl(s3URI, metadataStore, blobStore, Telemetry.NOOP);
    assertDoesNotThrow(
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                physicalIO,
                Telemetry.NOOP,
                LogicalIOConfiguration.DEFAULT,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT)));
  }

  @Test
  void testMetadataWithNegativeContentLength() {
    ObjectClient mockClient = mock(ObjectClient.class);
    when(mockClient.headObject(any(HeadRequest.class)))
        .thenReturn(
            CompletableFuture.completedFuture(ObjectMetadata.builder().contentLength(-1).build()));
    S3URI s3URI = S3URI.of("test", "test");
    MetadataStore metadataStore =
        new MetadataStore(mockClient, Telemetry.NOOP, PhysicalIOConfiguration.DEFAULT);
    BlobStore blobStore =
        new BlobStore(metadataStore, mockClient, Telemetry.NOOP, PhysicalIOConfiguration.DEFAULT);
    PhysicalIOImpl physicalIO = new PhysicalIOImpl(s3URI, metadataStore, blobStore, Telemetry.NOOP);
    assertDoesNotThrow(
        () ->
            new ParquetLogicalIOImpl(
                TEST_URI,
                physicalIO,
                Telemetry.NOOP,
                LogicalIOConfiguration.DEFAULT,
                new ParquetMetadataStore(LogicalIOConfiguration.DEFAULT)));
  }
}
