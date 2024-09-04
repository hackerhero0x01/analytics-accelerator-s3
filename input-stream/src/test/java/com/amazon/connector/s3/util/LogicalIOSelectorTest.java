package com.amazon.connector.s3.util;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.amazon.connector.s3.S3SeekableInputStreamConfiguration;
import com.amazon.connector.s3.TestTelemetry;
import com.amazon.connector.s3.io.logical.LogicalIO;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.impl.DefaultLogicalIOImpl;
import com.amazon.connector.s3.io.logical.impl.ParquetLogicalIOImpl;
import com.amazon.connector.s3.io.logical.impl.ParquetMetadataStore;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.io.physical.data.BlobStore;
import com.amazon.connector.s3.io.physical.data.MetadataStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class LogicalIOSelectorTest {

  private static MetadataStore metadataStore;
  private static BlobStore blobStore;

  @BeforeAll
  public static void setUp() {
    FakeObjectClient fakeObjectClient = new FakeObjectClient("content");
    metadataStore =
        new MetadataStore(fakeObjectClient, TestTelemetry.DEFAULT, PhysicalIOConfiguration.DEFAULT);
    blobStore =
        new BlobStore(
            metadataStore,
            fakeObjectClient,
            TestTelemetry.DEFAULT,
            PhysicalIOConfiguration.DEFAULT);
  }

  @ParameterizedTest
  @ValueSource(strings = {"key.parquet", "key.par"})
  public void testDefaultConfigParquetLogicalIOSelection(String key) {
    LogicalIO logicalIO =
        LogicalIOFactory.getLogicalIOImplementation(
            S3URI.of("bucket", key),
            metadataStore,
            blobStore,
            TestTelemetry.DEFAULT,
            S3SeekableInputStreamConfiguration.DEFAULT,
            mock(ParquetMetadataStore.class));

    assertTrue(logicalIO instanceof ParquetLogicalIOImpl);
  }

  @ParameterizedTest
  @ValueSource(strings = {"key.pr3", "key.par3"})
  public void testConfiguredExtensionParquetLogicalIOSelection(String key) {
    // Build with configuration that accepts ".pr3" and "par3" are parquet file extensions.
    LogicalIO logicalIO =
        LogicalIOFactory.getLogicalIOImplementation(
            S3URI.of("bucket", key),
            metadataStore,
            blobStore,
            TestTelemetry.DEFAULT,
            S3SeekableInputStreamConfiguration.builder()
                .logicalIOConfiguration(
                    LogicalIOConfiguration.builder().parquetFileSuffix("pr3|par3").build())
                .build(),
            mock(ParquetMetadataStore.class));

    assertTrue(logicalIO instanceof ParquetLogicalIOImpl);
  }

  @ParameterizedTest
  @ValueSource(strings = {"key.jar", "key.txt", "key.parque", "key.pa"})
  public void testNonParquetLogicalIOSelection(String key) {
    // Build with configuration that accepts ".pr3" and "par3" are parquet file extensions.
    LogicalIO logicalIO =
        LogicalIOFactory.getLogicalIOImplementation(
            S3URI.of("bucket", key),
            metadataStore,
            blobStore,
            TestTelemetry.DEFAULT,
            S3SeekableInputStreamConfiguration.DEFAULT,
            mock(ParquetMetadataStore.class));

    assertTrue(logicalIO instanceof DefaultLogicalIOImpl);
  }
}
