package com.amazon.connector.s3.util;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.request.Range;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;

public class FakeObjectClient implements ObjectClient {

  private final String content;

  @Getter private int headRequestCount = 0;
  @Getter private int getRequestCount = 0;

  /**
   * Instantiate a fake Object Client backed by some string as data.
   *
   * @param data the data making up the object
   */
  public FakeObjectClient(String data) {
    this.content = data;
  }

  @Override
  public CompletableFuture<ObjectMetadata> headObject(HeadRequest headRequest) {
    headRequestCount++;
    return CompletableFuture.completedFuture(
        ObjectMetadata.builder().contentLength(this.content.length()).build());
  }

  @Override
  public CompletableFuture<ObjectContent> getObject(GetRequest getRequest) {
    getRequestCount++;
    return CompletableFuture.completedFuture(
        ObjectContent.builder().stream(getTestInputStream(getRequest.getRange())).build());
  }

  @Override
  public void close() {
    // noop
  }

  private InputStream getTestInputStream(Range range) {
    byte[] requestedRange;
    if (Objects.isNull(range)) {
      requestedRange = this.content.getBytes(StandardCharsets.UTF_8);
    } else {
      byte[] data = this.content.getBytes(StandardCharsets.UTF_8);
      requestedRange = Arrays.copyOfRange(data, (int) range.getStart(), (int) range.getEnd() + 1);
    }

    return new ByteArrayInputStream(requestedRange);
  }
}
