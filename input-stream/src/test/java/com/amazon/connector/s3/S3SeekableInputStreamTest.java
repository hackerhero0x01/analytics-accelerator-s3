package com.amazon.connector.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.amazon.connector.s3.util.S3URI;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.utils.IoUtils;

public class S3SeekableInputStreamTest {

  private static final String TEST_DATA = "test-data12345678910";

  private class FakeObjectClient implements ObjectClient {

    @Override
    public HeadObjectResponse headObject(HeadObjectRequest headObjectRequest) {
      throw new RuntimeException("Not implemented.");
    }

    @Override
    public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest getObjectRequest) {
      return getTestResponseInputStream();
    }
  }

  private ResponseInputStream<GetObjectResponse> getTestResponseInputStream() {
    InputStream testStream = new ByteArrayInputStream(TEST_DATA.getBytes(StandardCharsets.UTF_8));
    GetObjectResponse response = GetObjectResponse.builder().build();
    return new ResponseInputStream<>(response, testStream);
  }

  private final FakeObjectClient fakeObjectClient = new FakeObjectClient();
  private final ObjectClient mockObjectClient = mock(S3SdkObjectClient.class);
  private static final S3URI TEST_OBJECT = S3URI.of("bucket", "key");

  @Test
  void testConstructor() throws IOException {
    ObjectClient objectClient = mock(ObjectClient.class);
    S3SeekableInputStream inputStream = new S3SeekableInputStream(objectClient, TEST_OBJECT);
    assertNotNull(inputStream);
  }

  @ParameterizedTest
  @MethodSource("constructorArgumentProvider")
  void testConstructorThrowsOnNullArgument(ObjectClient objectClient, S3URI s3URI) {
    assertThrows(
        NullPointerException.class,
        () -> {
          new S3SeekableInputStream(objectClient, s3URI);
        });
  }

  private static Stream<Arguments> constructorArgumentProvider() {
    return Stream.of(
        Arguments.of(null, S3URI.of("foo", "bar")), Arguments.of(mock(ObjectClient.class), null));
  }

  @Test
  void testInitialGetPosition() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(fakeObjectClient, TEST_OBJECT);

    // When: nothing
    // Then: stream position is at 0
    assertEquals(0, stream.getPos());
  }

  @Test
  void testReadAdvancesPosition() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(fakeObjectClient, TEST_OBJECT);

    // When: read() is called
    stream.read();

    // Then: position is advanced
    assertEquals(1, stream.getPos());
  }

  @Test
  void testSeek() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(mockObjectClient, TEST_OBJECT);

    // When
    stream.seek(1337);

    // Then
    assertEquals(1337, stream.getPos());
    verify(mockObjectClient, times(2)).getObject(any());
  }

  @Test
  void testSeekThrows() throws IOException {
    // Given: stream that throws on the first seek
    ObjectClient throwingObjectClient = mock(ObjectClient.class);
    when(throwingObjectClient.getObject(any()))
        .thenReturn(getTestResponseInputStream()) // first call is successful
        .thenThrow(
            new RuntimeException(
                "Could not seek for some underlying reason.")); // second call throws
    S3SeekableInputStream stream = new S3SeekableInputStream(throwingObjectClient, TEST_OBJECT);

    // When: seek() is called
    // Then: stream throws but position is not altered
    assertThrows(
        IOException.class,
        () -> {
          stream.seek(1337);
        });
    assertEquals(0, stream.getPos());
  }

  @Test
  void testFullRead() throws IOException {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(fakeObjectClient, TEST_OBJECT);

    // When: all data is requested
    String dataReadOut = IoUtils.toUtf8String(stream);

    // Then: data read out is the same as data under stream
    assertEquals(TEST_DATA, dataReadOut);
  }

  @Test
  void testCloseClosesUnderlyingStream() throws IOException {
    // Given: a seekable stream with a mocked underlying stream
    ObjectClient objectClient = mock(ObjectClient.class);
    ResponseInputStream<GetObjectResponse> underlyingStream = mock(ResponseInputStream.class);
    when(objectClient.getObject(any())).thenReturn(underlyingStream);
    S3SeekableInputStream stream = new S3SeekableInputStream(objectClient, TEST_OBJECT);

    // When: seekable stream closes
    stream.close();

    // Then: underlying stream gets closed
    verify(underlyingStream, times(1)).close();
  }
}
