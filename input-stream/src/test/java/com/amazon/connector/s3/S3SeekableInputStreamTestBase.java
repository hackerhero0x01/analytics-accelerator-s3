package com.amazon.connector.s3;

import com.amazon.connector.s3.io.logical.LogicalIO;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.impl.ParquetLogicalIOImpl;
import com.amazon.connector.s3.io.logical.impl.ParquetMetadataStore;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.io.physical.data.BlobStore;
import com.amazon.connector.s3.io.physical.data.MetadataStore;
import com.amazon.connector.s3.io.physical.impl.PhysicalIOImpl;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;

public class S3SeekableInputStreamTestBase {

  protected static final String TEST_DATA = "test-data12345678910";
  protected static final S3URI TEST_OBJECT = S3URI.of("bucket", "key");

  protected final PhysicalIOConfiguration physicalIOConfiguration = PhysicalIOConfiguration.DEFAULT;
  protected final FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
  protected final MetadataStore metadataStore =
      new MetadataStore(fakeObjectClient, PhysicalIOConfiguration.DEFAULT);
  protected final BlobStore blobStore =
      new BlobStore(metadataStore, fakeObjectClient, physicalIOConfiguration);
  protected final LogicalIOConfiguration logicalIOConfiguration = LogicalIOConfiguration.DEFAULT;

  protected final LogicalIO fakeLogicalIO =
      new ParquetLogicalIOImpl(
          TEST_OBJECT,
          new PhysicalIOImpl(TEST_OBJECT, metadataStore, blobStore),
          logicalIOConfiguration,
          new ParquetMetadataStore(logicalIOConfiguration));
}
