package com.amazon.connector.s3;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.amazon.connector.s3.util.S3SeekableInputStreamBuilder;
import com.amazon.connector.s3.util.S3URI;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class S3SeekableInputStreamFactoryTest {

  @Test
  void testConstructor() {
    S3SeekableInputStreamFactory inputStreamFactory =
        new S3SeekableInputStreamFactory(S3SeekableInputStreamBuilder.builder().build());
    assertNotNull(inputStreamFactory);

    S3SeekableInputStreamFactory inputStreamFactoryWrappedClient =
        new S3SeekableInputStreamFactory(
            S3SeekableInputStreamBuilder.builder()
                .wrappedAsyncClient(S3AsyncClient.create())
                .build());
    assertNotNull(inputStreamFactoryWrappedClient);
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new S3SeekableInputStreamFactory((S3SeekableInputStreamBuilder) null);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new S3SeekableInputStreamFactory((S3SdkObjectClient) null);
        });
  }

  @Test
  void testCreateStream() {

    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(S3SeekableInputStreamBuilder.builder().build());

    S3SeekableInputStream inputStream =
        s3SeekableInputStreamFactory.createStream(S3URI.of("bucket", "key"));

    assertNotNull(inputStream);
  }

  @Test
  void testCreateStreamThrowsOnNullArgument() {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(S3SeekableInputStreamBuilder.builder().build());

    assertThrows(
        NullPointerException.class,
        () -> {
          s3SeekableInputStreamFactory.createStream(null);
        });
  }

  @Test
  void testObjectClientGetsClosed() throws Exception {

    S3SdkObjectClient s3SdkObjectClient = mock(S3SdkObjectClient.class);

    // Given
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(s3SdkObjectClient);

    // When
    s3SeekableInputStreamFactory.close();

    // Then
    verify(s3SdkObjectClient, times(1)).close();
  }
}
