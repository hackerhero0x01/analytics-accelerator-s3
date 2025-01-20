/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.s3.analyticsaccelerator.exceptions.ExceptionHandler;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.DefaultLogicalIOImpl;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetLogicalIOImpl;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class S3SeekableInputStreamFactoryTest {

  private static final S3URI TEST_URI = S3URI.of("test-bucket", "test-key");

  @Test
  void testConstructor() {
    ObjectClient objectClient = mock(ObjectClient.class);
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(objectClient, S3SeekableInputStreamConfiguration.DEFAULT);
    assertEquals(
        S3SeekableInputStreamConfiguration.DEFAULT,
        s3SeekableInputStreamFactory.getConfiguration());
    assertEquals(objectClient, s3SeekableInputStreamFactory.getObjectClient());
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new S3SeekableInputStreamFactory(null, S3SeekableInputStreamConfiguration.DEFAULT);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new S3SeekableInputStreamFactory(mock(ObjectClient.class), null);
        });
  }

  @Test
  void testCreateDefaultStream() {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class),
            S3SeekableInputStreamConfiguration.builder()
                .logicalIOConfiguration(
                    LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
                .build());
    S3SeekableInputStream inputStream =
        s3SeekableInputStreamFactory.createStream(S3URI.of("bucket", "key"));
    assertNotNull(inputStream);

    inputStream =
        s3SeekableInputStreamFactory.createStream(
            S3URI.of("bucket", "key"), mock(StreamContext.class));
    assertNotNull(inputStream);
  }

  @Test
  void testCreateStreamWithContentLength() {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class),
            S3SeekableInputStreamConfiguration.builder()
                .logicalIOConfiguration(
                    LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
                .build());
    S3SeekableInputStream inputStream =
        s3SeekableInputStreamFactory.createStream(S3URI.of("bucket", "key"), 500);
    assertNotNull(inputStream);
  }

  @Test
  void testCreateIndependentStream() {
    S3SeekableInputStreamConfiguration configuration =
        S3SeekableInputStreamConfiguration.builder()
            .logicalIOConfiguration(
                LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
            .build();
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(mock(ObjectClient.class), configuration);
    S3SeekableInputStream inputStream =
        s3SeekableInputStreamFactory.createStream(S3URI.of("bucket", "key"));
    assertNotNull(inputStream);

    inputStream =
        s3SeekableInputStreamFactory.createStream(
            S3URI.of("bucket", "key"), mock(StreamContext.class));
    assertNotNull(inputStream);
  }

  @Test
  void testCreateStreamThrowsOnNullArgument() {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class), S3SeekableInputStreamConfiguration.DEFAULT);
    assertThrows(
        NullPointerException.class,
        () -> {
          s3SeekableInputStreamFactory.createStream(null);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          s3SeekableInputStreamFactory.createStream(null, mock(StreamContext.class));
        });
  }

  @Test
  void testCreateLogicalIO() {
    S3SeekableInputStreamConfiguration configuration =
        S3SeekableInputStreamConfiguration.builder()
            .logicalIOConfiguration(
                LogicalIOConfiguration.builder().prefetchFooterEnabled(false).build())
            .build();
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(mock(ObjectClient.class), configuration);

    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(
                S3URI.of("bucket", "key.parquet"), mock(StreamContext.class))
            instanceof ParquetLogicalIOImpl);
    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(
                S3URI.of("bucket", "key.par"), mock(StreamContext.class))
            instanceof ParquetLogicalIOImpl);

    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(
                S3URI.of("bucket", "key.java"), mock(StreamContext.class))
            instanceof DefaultLogicalIOImpl);
    assertTrue(
        s3SeekableInputStreamFactory.createLogicalIO(
                S3URI.of("bucket", "key.txt"), mock(StreamContext.class))
            instanceof DefaultLogicalIOImpl);
  }

  @Test
  void testClose() {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            mock(ObjectClient.class), S3SeekableInputStreamConfiguration.DEFAULT);
    assertDoesNotThrow(() -> s3SeekableInputStreamFactory.close());
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @MethodSource("exceptions")
  void testHeadObjectExceptions(Exception exception) {
    S3AsyncClient mockS3AsyncClient = mock(S3AsyncClient.class);
    CompletableFuture<HeadObjectResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(exception);
    when(mockS3AsyncClient.headObject(any(HeadObjectRequest.class))).thenReturn(failedFuture);

    assertInputStreamReadExceptions(exception, mockS3AsyncClient);
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @MethodSource("exceptions")
  void testGetObjectExceptions(Exception exception) {
    S3AsyncClient mockS3AsyncClient = mock(S3AsyncClient.class);
    // As headObject call happens first, we make a successful headObject mocking so that failure
    // gets triggered only at the getObject
    CompletableFuture<HeadObjectResponse> successfulFuture = new CompletableFuture<>();
    successfulFuture.complete(HeadObjectResponse.builder().contentLength(1L).build());
    when(mockS3AsyncClient.headObject(any(HeadObjectRequest.class))).thenReturn(successfulFuture);

    CompletableFuture<ResponseInputStream<GetObjectResponse>> failedFuture =
        new CompletableFuture<>();
    failedFuture.completeExceptionally(exception);
    when(mockS3AsyncClient.getObject(
            any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
        .thenReturn(failedFuture);

    assertInputStreamReadExceptions(exception, mockS3AsyncClient);
  }

  private static void assertInputStreamReadExceptions(
      final Exception expectedException, final S3AsyncClient mockS3AsyncClient) {
    S3SeekableInputStreamFactory factory =
        new S3SeekableInputStreamFactory(
            new S3SdkObjectClient(mockS3AsyncClient), S3SeekableInputStreamConfiguration.DEFAULT);
    S3SeekableInputStream inputStream = factory.createStream(TEST_URI, mock(StreamContext.class));
    Exception thrownException = assertThrows(Exception.class, inputStream::read);
    assertInstanceOf(IOException.class, thrownException);
    Optional.ofNullable(thrownException.getCause())
        .ifPresent(
            underlyingException ->
                assertInstanceOf(expectedException.getClass(), underlyingException));
  }

  private static Exception[] exceptions() {
    return ExceptionHandler.getSampleExceptions();
  }
}
