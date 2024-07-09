package com.amazon.connector.s3.reference;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.amazon.connector.s3.S3SdkObjectClient;
import com.amazon.connector.s3.S3SeekableInputStream;
import com.amazon.connector.s3.S3SeekableInputStreamConfiguration;
import com.amazon.connector.s3.S3SeekableInputStreamFactory;
import com.amazon.connector.s3.arbitraries.StreamArbitraries;
import com.amazon.connector.s3.model.InMemorySeekableStream;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.BeforeContainer;
import net.jqwik.testcontainers.Container;
import net.jqwik.testcontainers.Testcontainers;
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
public class S3MockVsInMemoryReferenceTest extends StreamArbitraries {

  private static final String S3MOCK_VERSION = "latest";
  private static final Collection<String> INITIAL_BUCKET_NAMES = asList("bucket");

  // Container will be started before each test method and stopped after
  @Container
  private static final S3MockContainer S3_MOCK =
      new S3MockContainer(S3MOCK_VERSION)
          .withInitialBuckets(String.join(",", INITIAL_BUCKET_NAMES));

  private static S3AsyncClient s3Client;
  private S3SeekableInputStream s3SeekableInputStream;
  private InMemorySeekableStream inMemorySeekableStream;
  private static final String TEST_BUCKET = "bucket";
  private static S3SeekableInputStreamFactory s3SeekableInputStreamFactory;

  @BeforeContainer
  static void setup() {
    s3Client = createS3ClientV2(S3_MOCK.getHttpsEndpoint());
    // Initialise streams
    s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            new S3SdkObjectClient(s3Client), S3SeekableInputStreamConfiguration.DEFAULT);
  }

  @AfterContainer
  static void teardown() throws IOException {
    s3SeekableInputStreamFactory.close();
  }

  void setupStreams(int size) throws IOException {
    // Generate random data
    Random r = new Random();
    byte[] data = new byte[size];
    r.nextBytes(data);

    // Put random data in S3
    String uuidKey = UUID.randomUUID().toString();
    S3URI uri = S3URI.of(TEST_BUCKET, uuidKey);

    s3Client
        .putObject(x -> x.bucket(TEST_BUCKET).key(uuidKey), AsyncRequestBody.fromBytes(data))
        .join();

    s3SeekableInputStream = s3SeekableInputStreamFactory.createStream(uri);
    inMemorySeekableStream = new InMemorySeekableStream(data);
  }

  /**
   * This test, while not a reference test, is useful for catching issues with the S3 Mock +
   * SeekableStream setup early. This test failing is a strong indication that something is majorly
   * wrong with the setup which is likely also breaking the other tests.
   */
  @Example
  public void regressionTest_S3MockInitThenSeekThenRead() throws IOException {
    setupStreams(100);

    s3SeekableInputStream.seek(0);
    s3SeekableInputStream.getPos();
    s3SeekableInputStream.read();
  }

  @Property
  public void testPositionInitiallyZero(@ForAll("positiveStreamSizes") int size)
      throws IOException {
    setupStreams(size);

    assertEquals(0, inMemorySeekableStream.getPos());
    assertEquals(
        inMemorySeekableStream.getPos(),
        s3SeekableInputStream.getPos(),
        "getPos() of both implementations should match");
  }

  @Property
  public void testReadAndSeek_matchesWithReference(
      @ForAll("positiveStreamSizes") int size, @ForAll("validPositions") int pos)
      throws IOException {
    setupStreams(size);

    int validJump = pos % size;
    s3SeekableInputStream.seek(validJump);
    inMemorySeekableStream.seek(validJump);

    assertEquals(
        s3SeekableInputStream.getPos(),
        inMemorySeekableStream.getPos(),
        String.format("positions do not match after seeking to %s", validJump));
    assertEquals(
        s3SeekableInputStream.read(),
        inMemorySeekableStream.read(),
        String.format("returned data does not match after seeking to %s", validJump));
  }

  @Property
  public void testReadWithBuffer_matchesWithReference(
      @ForAll("positiveStreamSizes") int streamSize,
      @ForAll("validPositions") int pos,
      @ForAll("bufferSizes") int bufSize)
      throws IOException {
    setupStreams(streamSize);

    int validJump = pos % streamSize;
    s3SeekableInputStream.seek(validJump);
    inMemorySeekableStream.seek(validJump);

    assertEquals(
        s3SeekableInputStream.getPos(),
        inMemorySeekableStream.getPos(),
        String.format("positions do not match after seeking to %s", validJump));

    byte[] b1 = new byte[bufSize];
    byte[] b2 = new byte[bufSize];

    assertEquals(
        s3SeekableInputStream.read(b1, 0, bufSize),
        inMemorySeekableStream.read(b2, 0, bufSize),
        "number of bytes read should be the same");

    assertEquals(
        byteBufToString(b1),
        byteBufToString(b2),
        String.format("returned data does not match after requesting %s bytes", bufSize));

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

  @Property
  public void testFullRead(@ForAll("positiveStreamSizes") int size) throws IOException {
    setupStreams(size);

    String seekableFullRead = IoUtils.toUtf8String(s3SeekableInputStream);
    String inMemoryFullRead = IoUtils.toUtf8String(inMemorySeekableStream);

    assertEquals(seekableFullRead, inMemoryFullRead);
  }

  @Property
  public void testTailReads(
      @ForAll("positiveStreamSizes") int streamSize,
      @ForAll("validPositions") int pos,
      @ForAll("bufferSizes") int bufSize)
      throws IOException {
    setupStreams(streamSize);

    int validJump = pos % streamSize;
    s3SeekableInputStream.seek(validJump);
    inMemorySeekableStream.seek(validJump);

    assertEquals(
        s3SeekableInputStream.getPos(),
        inMemorySeekableStream.getPos(),
        String.format("positions do not match after seeking to %s", validJump));

    int validBufSize = 1 + bufSize % streamSize;
    byte[] b1 = new byte[validBufSize];
    byte[] b2 = new byte[validBufSize];

    assertEquals(
        s3SeekableInputStream.readTail(b1, 0, validBufSize),
        inMemorySeekableStream.readTail(b2, 0, validBufSize),
        "number of bytes read from tail should be the same");

    assertEquals(
        byteBufToString(b1),
        byteBufToString(b2),
        String.format(
            "returned data does not match after requesting %s bytes from tail", validBufSize));

    assertEquals(
        s3SeekableInputStream.read(),
        inMemorySeekableStream.read(),
        String.format("read() calls followed by a readTail do not return the same data"));
  }

  private String byteBufToString(byte[] b) {
    return new String(b, StandardCharsets.UTF_8);
  }

  private static S3AsyncClient createS3ClientV2(String endpoint) {
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
}
