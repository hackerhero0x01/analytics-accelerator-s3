package com.amazon.connector.s3;

import com.amazon.connector.s3.util.S3URI;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * High throughput seekable stream used to read data from Amazon S3.
 *
 * <p>Don't share between threads. The current implementation is not thread safe in that calling
 * {@link #seek(long) seek} will modify the position of the stream and the behaviour of calling
 * {@link #seek(long) seek} and {@link #read() read} concurrently from two different threads is
 * undefined.
 */
public class S3SeekableInputStream extends SeekableInputStream {
  private final ObjectClient objectClient;
  private final S3URI uri;

  private long position;
  private InputStream stream;

  /**
   * Creates a new instance of {@link S3SeekableInputStream}.
   *
   * @param objectClient an instance of {@link ObjectClient}.
   * @param uri location of the S3 object this stream is fetching data from
   */
  public S3SeekableInputStream(ObjectClient objectClient, S3URI uri) throws IOException {
    Preconditions.checkNotNull(objectClient, "objectClient must not be null");
    Preconditions.checkNotNull(uri, "S3 URI must not be null");

    this.objectClient = objectClient;
    this.uri = uri;

    this.position = 0;
    requestBytes(position);
  }

  @Override
  public int read() throws IOException {
    int byteRead = stream.read();

    if (byteRead < 0) {
      return -1;
    }

    this.position++;
    return byteRead;
  }

  @Override
  public void seek(long pos) throws IOException {
    try {
      requestBytes(pos);
      this.position = pos;
    } catch (Exception e) {
      throw new IOException(String.format("Unable to seek to position %s", pos));
    }
  }

  @Override
  public long getPos() {
    return this.position;
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.stream.close();
  }

  private void requestBytes(long pos) throws IOException {
    if (Objects.nonNull(this.stream)) {
      this.stream.close();
    }

    this.stream =
        this.objectClient.getObject(
            GetObjectRequest.builder()
                .bucket(uri.getBucket())
                .key(uri.getKey())
                .range(String.format("bytes=%s-", pos))
                .build());
  }
}
