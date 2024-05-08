package com.amazon.connector.s3;

import com.amazon.connector.s3.util.S3SeekableInputStreamBuilder;
import com.amazon.connector.s3.util.S3URI;
import lombok.NonNull;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Initialises resources to prepare for reading from S3. Resources initialised in this class are
 * shared across instances of {@link S3SeekableInputStream}. For example, this allows for the same
 * S3 client to be used across multiple input streams for more efficient connection management etc.
 * Callers must close() to release any shared resources. Closing {@link S3SeekableInputStream} will
 * only release underlying resources held by the stream.
 */
public class S3SeekableInputStreamFactory implements AutoCloseable {

  S3SdkObjectClient s3SdkObjectClient;

  /**
   * Creates a new instance of {@link S3SeekableInputStreamFactory}. This factory should be used to
   * create instances of the input stream to allow for sharing resources such as the object client
   * between streams.
   *
   * @param s3SeekableInputStreamBuilder s3SeekableInputStreamBuilder
   */
  public S3SeekableInputStreamFactory(
      @NonNull S3SeekableInputStreamBuilder s3SeekableInputStreamBuilder) {

    S3AsyncClient s3AsyncClient = s3SeekableInputStreamBuilder.getWrappedAsyncClient();

    // If no wrapped client is provided, defaults to using the S3 CRT client.
    if (s3AsyncClient != null) {
      this.s3SdkObjectClient =
          new S3SdkObjectClient(s3SeekableInputStreamBuilder.getWrappedAsyncClient());
    } else {
      this.s3SdkObjectClient = new S3SdkObjectClient();
    }
  }

  /***
   * Given an object client, creates a new instance of {@link S3SeekableInputStreamFactory}. This
   * version of the constructor is for testing purposes only and to allow for dependency injection.
   *
   * @param s3SdkObjectClient The object client to use
   */
  protected S3SeekableInputStreamFactory(@NonNull S3SdkObjectClient s3SdkObjectClient) {
    this.s3SdkObjectClient = s3SdkObjectClient;
  }

  /**
   * Create an instance of S3SeekableInputStream.
   *
   * @param s3URI the object's S3 URI
   * @return An instance of the input stream.
   */
  public S3SeekableInputStream createStream(@NonNull S3URI s3URI) {
    return new S3SeekableInputStream(s3SdkObjectClient, s3URI);
  }

  @Override
  public void close() throws Exception {
    this.s3SdkObjectClient.close();
  }
}
