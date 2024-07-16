package com.amazon.connector.s3.property;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.S3SeekableInputStreamConfiguration;
import com.amazon.connector.s3.S3SeekableInputStreamFactory;
import com.amazon.connector.s3.SeekableInputStream;
import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.util.S3URI;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

public class InMemoryS3SeekableInputStream extends SeekableInputStream {

  private final SeekableInputStream delegate;

  /**
   * Returns an in memory S3 seekable stream. Pretends to point at s3://bucket/key and generates
   * `len` number of random bytes which will be then served back as object bytes.
   *
   * @param bucket the bucket
   * @param key the key
   * @param len the length of the data
   */
  public InMemoryS3SeekableInputStream(String bucket, String key, int len) {
    S3URI s3URI = S3URI.of(bucket, key);
    ObjectClient objectClient = new InMemoryObjectClient(len);

    S3SeekableInputStreamFactory factory =
        new S3SeekableInputStreamFactory(objectClient, S3SeekableInputStreamConfiguration.DEFAULT);
    this.delegate = factory.createStream(s3URI);
  }

  private static class InMemoryObjectClient implements ObjectClient {

    private final int size;
    private byte[] content;

    public InMemoryObjectClient(int size) {
      this.size = size;
      this.content = new byte[size];

      // Fill with random bytes
      Random r = new Random();
      r.nextBytes(this.content);
    }

    @Override
    public CompletableFuture<ObjectMetadata> headObject(HeadRequest headRequest) {
      return CompletableFuture.completedFuture(
          ObjectMetadata.builder().contentLength(size).build());
    }

    @Override
    public CompletableFuture<ObjectContent> getObject(GetRequest getRequest) {
      int start = 0;
      int end = size - 1;

      if (Objects.nonNull(getRequest.getRange())) {
        start = (int) getRequest.getRange().getStart();
        end = (int) getRequest.getRange().getEnd();
      }

      byte[] range = Arrays.copyOfRange(this.content, start, end + 1);
      return CompletableFuture.completedFuture(
          ObjectContent.builder().stream(new ByteArrayInputStream(range)).build());
    }

    @Override
    public void close() throws IOException {
      this.content = null;
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    this.delegate.seek(pos);
  }

  @Override
  public long getPos() {
    return this.delegate.getPos();
  }

  @Override
  public int readTail(byte[] buf, int off, int n) throws IOException {
    return this.delegate.readTail(buf, off, n);
  }

  @Override
  public int read() throws IOException {
    return this.delegate.read();
  }
}
