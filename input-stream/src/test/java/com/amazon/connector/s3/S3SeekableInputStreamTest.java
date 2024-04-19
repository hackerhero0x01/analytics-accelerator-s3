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

public class S3SeekableInputStreamTest {

  private class FakeObjectClient implements ObjectClient {

    @Override
    public HeadObjectResponse headObject(HeadObjectRequest headObjectRequest) {
      throw new RuntimeException("Not implemented.");
    }

    @Override
    public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest getObjectRequest) {
      InputStream testStream =
          new ByteArrayInputStream("test-data".getBytes(StandardCharsets.UTF_8));
      GetObjectResponse response = GetObjectResponse.builder().build();
      return new ResponseInputStream<>(response, testStream);
    }
  }

  private final FakeObjectClient fakeObjectClient = new FakeObjectClient();
  private final ObjectClient objectClient = mock(S3SdkObjectClient.class);
  private static final S3URI TEST_OBJECT = S3URI.of("bucket", "key");

  @Test
  void testConstructor() {
    ObjectClient objectClient = mock(ObjectClient.class);
    S3SeekableInputStream inputStream = new S3SeekableInputStream(objectClient, TEST_OBJECT);
    assertNotNull(inputStream);
  }

  @ParameterizedTest
  @MethodSource("constructorArgumentProvider")
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new S3SeekableInputStream(null, null);
        });
  }

  private static Stream<Arguments> constructorArgumentProvider() {
    return Stream.of(
        Arguments.of(null, S3URI.of("foo", "bar")),
        Arguments.of(mock(ObjectClient.class), S3URI.of(null, "bar")),
        Arguments.of(mock(ObjectClient.class), S3URI.of("foo", null)),
        Arguments.of(mock(ObjectClient.class), S3URI.of(null, null)));
  }

  @Test
  void testInitialGetPosition() {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(objectClient, TEST_OBJECT);

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
  void testSeek() {
    // Given
    S3SeekableInputStream stream = new S3SeekableInputStream(objectClient, TEST_OBJECT);

    // When
    stream.seek(1337);

    // Then
    assertEquals(1337, stream.getPos());
    verify(objectClient, times(2)).getObject(any());
  }
}
