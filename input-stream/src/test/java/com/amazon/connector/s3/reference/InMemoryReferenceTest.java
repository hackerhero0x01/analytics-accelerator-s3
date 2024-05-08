package com.amazon.connector.s3.reference;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.amazon.connector.s3.S3SeekableInputStream;
import com.amazon.connector.s3.S3SeekableInputStreamFactory;
import com.amazon.connector.s3.util.S3SeekableInputStreamBuilder;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.awssdk.utils.IoUtils;

@Testcontainers
public class InMemoryReferenceTest {

  private static final String S3MOCK_VERSION = "latest";
  private static final Collection<String> INITIAL_BUCKET_NAMES = asList("bucket");

  // Container will be started before each test method and stopped after
  @Container
  private final S3MockContainer s3Mock =
      new S3MockContainer(S3MOCK_VERSION)
          .withInitialBuckets(String.join(",", INITIAL_BUCKET_NAMES));

  private S3AsyncClient s3Client;
  private S3SeekableInputStream s3SeekableInputStream;
  private InMemorySeekableStream inMemorySeekableStream;

  private S3AsyncClient createS3ClientV2(String endpoint) {
    return S3AsyncClient.builder()
        .region(Region.of("us-east-1"))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
        // Path style access is a must because S3Mock cannot support virtual hosted style (as it
        // runs under localhost)
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .endpointOverride(URI.create(endpoint))
        .httpClient(
            NettyNioAsyncHttpClient.builder()
                .buildWithDefaults(
                    AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, Boolean.TRUE).build()))
        .build();
  }

  private static final int ONE_MB = 1024 * 1024;
  // 24 MB -- should force seekable streams to use more than a single IOBlock
  private static final int OBJECT_SIZE = 24 * ONE_MB;

  private static final String TEST_KEY = "key";
  private static final String TEST_BUCKET = "bucket";
  private static final S3URI TEST_URI = S3URI.of(TEST_BUCKET, TEST_KEY);
  private static S3SeekableInputStreamFactory s3SeekableInputStreamFactory;

  @BeforeEach
  void setup() throws IOException {
    // Generate random data
    Random r = new Random();
    byte[] data = new byte[OBJECT_SIZE];
    r.nextBytes(data);

    // Put random data in S3
    s3Client = createS3ClientV2(s3Mock.getHttpsEndpoint());
    s3Client
        .putObject(
            x -> x.bucket(TEST_URI.getBucket()).key(TEST_URI.getKey()),
            AsyncRequestBody.fromBytes(data))
        .join();

    // Initialise streams
    s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            S3SeekableInputStreamBuilder.builder().wrappedAsyncClient(s3Client).build());
    s3SeekableInputStream = s3SeekableInputStreamFactory.createStream(TEST_URI);
    inMemorySeekableStream = new InMemorySeekableStream(data);
  }

  @Test
  /**
   * This test while not a reference test, is useful for catching issues with the S3 Mock +
   * SeekableStream setup early. This test failing is a strong indication that something is majorly
   * wrong with the setup which is likely also breaking the other tests.
   */
  public void regressionTest_S3MockInitThenSeekThenRead() throws IOException {
    s3SeekableInputStream.seek(0);
    s3SeekableInputStream.getPos();
    s3SeekableInputStream.read();
  }

  @Test
  public void testInit() {
    assertEquals(0, inMemorySeekableStream.getPos());
    assertEquals(
        inMemorySeekableStream.getPos(),
        s3SeekableInputStream.getPos(),
        "getPos() of both implementations should match");
  }

  @Test
  public void testReadAndSeek() throws IOException {
    Random r = new Random();
    for (int i = 0, nextPos = 0; i < 10; nextPos = nextRandomPosition(r), ++i) {
      s3SeekableInputStream.seek(nextPos);
      inMemorySeekableStream.seek(nextPos);

      assertEquals(
          s3SeekableInputStream.getPos(),
          inMemorySeekableStream.getPos(),
          String.format("positions do not match after seeking to %s", nextPos));
      assertEquals(
          s3SeekableInputStream.read(),
          inMemorySeekableStream.read(),
          String.format("returned data does not match after seeking to %s", nextPos));
    }
  }

  @Test
  public void testReadWithBuffer() throws IOException {
    Random r = new Random();
    for (int i = 0, nextPos = 0; i < 10; nextPos = nextRandomPosition(r), ++i) {
      s3SeekableInputStream.seek(nextPos);
      inMemorySeekableStream.seek(nextPos);

      assertEquals(
          s3SeekableInputStream.getPos(),
          inMemorySeekableStream.getPos(),
          String.format("positions do not match after seeking to %s", nextPos));

      int n = nextRandomSize(r);
      byte[] b1 = new byte[n];
      byte[] b2 = new byte[n];

      assertEquals(
          s3SeekableInputStream.read(b1, 0, n),
          inMemorySeekableStream.read(b2, 0, n),
          "number of bytes read should be the same");

      assertEquals(
          byteBufToString(b1),
          byteBufToString(b2),
          String.format("returned data does not match after requesting %s bytes", n));

      assertEquals(
          s3SeekableInputStream.getPos(),
          inMemorySeekableStream.getPos(),
          String.format("positions do not match after reading"));

      assertEquals(
          s3SeekableInputStream.read(),
          inMemorySeekableStream.read(),
          String.format(
              "read() calls followed by a read(buff, off, len) do not return the same data"));
    }
  }

  @Test
  public void testFullRead() throws IOException {
    String seekableFullRead = IoUtils.toUtf8String(s3SeekableInputStream);
    String inMemoryFullRead = IoUtils.toUtf8String(inMemorySeekableStream);

    assertEquals(seekableFullRead, inMemoryFullRead);
  }

  @Test
  public void testTailReads() throws IOException {
    Random r = new Random();
    for (int i = 0, nextPos = 0; i < 10; nextPos = nextRandomPosition(r), ++i) {
      s3SeekableInputStream.seek(nextPos);
      inMemorySeekableStream.seek(nextPos);

      assertEquals(
          s3SeekableInputStream.getPos(),
          inMemorySeekableStream.getPos(),
          String.format("positions do not match after seeking to %s", nextPos));

      int n = nextRandomSize(r);
      byte[] b1 = new byte[n];
      byte[] b2 = new byte[n];

      assertEquals(
          s3SeekableInputStream.readTail(b1, 0, n),
          inMemorySeekableStream.readTail(b2, 0, n),
          "number of bytes read from tail should be the same");

      assertEquals(
          byteBufToString(b1),
          byteBufToString(b2),
          String.format("returned data does not match after requesting %s bytes from tail", n));

      assertEquals(
          s3SeekableInputStream.read(),
          inMemorySeekableStream.read(),
          String.format("read() calls followed by a readTail do not return the same data"));
    }
  }

  private String byteBufToString(byte[] b) {
    return new String(b, StandardCharsets.UTF_8);
  }

  private int nextRandomSize(Random r) {
    return Math.abs(r.nextInt()) % OBJECT_SIZE;
  }

  private int nextRandomPosition(Random r) {
    return Math.abs(r.nextInt()) % OBJECT_SIZE;
  }
}
