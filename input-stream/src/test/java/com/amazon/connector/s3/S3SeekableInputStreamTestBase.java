package com.amazon.connector.s3;

import com.amazon.connector.s3.blockmanager.BlockManager;
import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.util.S3URI;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class S3SeekableInputStreamTestBase {

  protected static final String TEST_DATA = "test-data12345678910";
  protected static final S3URI TEST_OBJECT = S3URI.of("bucket", "key");
  protected final FakeObjectClient fakeObjectClient = new FakeObjectClient();
  protected final BlockManager fakeBlockManager = new BlockManager(fakeObjectClient, TEST_OBJECT);

  private class FakeObjectClient implements ObjectClient {

    @Override
    public CompletableFuture<ObjectMetadata> headObject(HeadRequest headRequest) {
      return CompletableFuture.completedFuture(
          ObjectMetadata.builder().contentLength((long) TEST_DATA.length()).build());
    }

    @Override
    public CompletableFuture<ObjectContent> getObject(GetRequest getRequest) {
      return CompletableFuture.completedFuture(
          ObjectContent.builder().stream(getTestInputStream(getRequest.getRange())).build());
    }

    @Override
    public void close() {
      // noop
    }
  }

  private InputStream getTestInputStream(Range range) {
    byte[] requestedRange;
    if (Objects.isNull(range)) {
      requestedRange = TEST_DATA.getBytes(StandardCharsets.UTF_8);
    } else {
      byte[] data = TEST_DATA.getBytes(StandardCharsets.UTF_8);
      requestedRange = Arrays.copyOfRange(data, (int) range.getStart(), (int) range.getEnd() + 1);
    }

    return new ByteArrayInputStream(requestedRange);
  }
}
