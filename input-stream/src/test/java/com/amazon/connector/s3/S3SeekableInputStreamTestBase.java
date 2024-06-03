package com.amazon.connector.s3;

import com.amazon.connector.s3.io.logical.LogicalIO;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.impl.ParquetLogicalIOImpl;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManager;
import com.amazon.connector.s3.io.physical.blockmanager.BlockManagerConfiguration;
import com.amazon.connector.s3.io.physical.impl.PhysicalIOImpl;
import com.amazon.connector.s3.util.FakeObjectClient;
import com.amazon.connector.s3.util.S3URI;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

public class S3SeekableInputStreamTestBase {

  protected static final String TEST_DATA = "test-data12345678910";
  protected static final S3URI TEST_OBJECT = S3URI.of("bucket", "key");

  protected final FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
  protected final BlockManager fakeBlockManager =
      new BlockManager(fakeObjectClient, TEST_OBJECT, BlockManagerConfiguration.DEFAULT);

  protected final LogicalIO fakeLogicalIO =
      new ParquetLogicalIOImpl(
          new PhysicalIOImpl(fakeBlockManager), LogicalIOConfiguration.DEFAULT);
}
