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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;

public class FakeObjectClient implements ObjectClient {

  private final String content;

  @Getter private int headRequestCount = 0;
  @Getter private int getRequestCount = 0;
  @Getter private List<Range> requestedRanges = new ArrayList<>();
  private byte[] contentBytes;

  /**
   * Instantiate a fake Object Client backed by some string as data.
   *
   * @param data the data making up the object
   */
  public FakeObjectClient(String data) {
    this.content = data;
    this.contentBytes = this.content.getBytes(StandardCharsets.UTF_8);
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
    requestedRanges.add(getRequest.getRange());
    return CompletableFuture.completedFuture(
        ObjectContent.builder().stream(getTestInputStream(getRequest.getRange())).build());
  }

  @Override
  public void close() {
    // noop
  }

  private InputStream getTestInputStream(Range range) {
    byte[] requestedRange =
        Arrays.copyOfRange(
            contentBytes,
            (int) range.getStart().orElse(0),
            (int) range.getEnd().orElse(this.content.length() - 1) + 1);

    return new ByteArrayInputStream(requestedRange);
  }
}
