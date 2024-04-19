package com.amazon.connector.s3;

import com.amazon.connector.s3.util.S3URI;
import com.google.common.base.Preconditions;
import java.io.IOException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/** High throughput stream used to read data from Amazon S3. */
public class S3SeekableInputStream extends SeekableInputStream {
  private final ObjectClient objectClient;
  private final S3URI uri;

  private long position;
  private ResponseInputStream<GetObjectResponse> stream;

  /**
   * Creates a new instance of {@link S3SeekableInputStream}.
   *
   * @param objectClient an instance of {@link ObjectClient}.
   */
  public S3SeekableInputStream(ObjectClient objectClient, S3URI uri) {
    Preconditions.checkNotNull(objectClient, "objectClient must not be null");
    Preconditions.checkNotNull(uri.bucket(), "S3 bucket must not be null");
    Preconditions.checkNotNull(uri.key(), "S3 key must not be null");

    this.objectClient = objectClient;
    this.uri = uri;

    this.position = 0;
    requestBytes(position);
  }

  @Override
  public int read() throws IOException {
    position++;
    return stream.read();
  }

  @Override
  public void seek(long pos) {
    this.position = pos;
    requestBytes(pos);
  }

  @Override
  public long getPos() {
    return this.position;
  }

  private void requestBytes(long pos) {
    this.stream =
        this.objectClient.getObject(
            GetObjectRequest.builder()
                .bucket(uri.bucket())
                .key(uri.key())
                .range(String.format("bytes=%s-", pos))
                .build());
  }
}
