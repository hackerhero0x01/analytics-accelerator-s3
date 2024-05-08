package com.amazon.connector.s3;

import com.amazon.connector.s3.util.S3SeekableInputStreamBuilder;
import com.amazon.connector.s3.util.S3URI;
import lombok.NonNull;

/**
 * Initialises resources to prepare for reading from S3. Resources initialised in this class are
 * shared across instances of {@link S3SeekableInputStream}. For example, this allows for the same
 * S3 client to be used across multiple input streams for more efficient connection management etc.
 * Callers must close() torelease any shared resources. Closing {@link S3SeekableInputStream} will
 * only release underlying resources held by the stream.
 */
public class S3SeekableInputStreamInitializer implements AutoCloseable {

  S3SdkObjectClient s3SdkObjectClient;

  public S3SeekableInputStreamInitializer(
      @NonNull S3SeekableInputStreamBuilder s3SeekableInputStreamBuilder) {
    this.s3SdkObjectClient =
        new S3SdkObjectClient(s3SeekableInputStreamBuilder.getWrappedAsyncClient());
  }

  public S3SeekableInputStream createStream(@NonNull S3URI s3URI) {
    return new S3SeekableInputStream(s3SdkObjectClient, s3URI);
  }

  @Override
  public void close() throws Exception {
    this.s3SdkObjectClient.close();
  }
}
