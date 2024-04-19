package com.amazon.connector.s3.reference;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.S3SdkObjectClient;
import com.amazon.connector.s3.S3SeekableInputStream;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
  private ObjectClient objectClient;
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

  private static final int OBJECT_SIZE = 1024 * 1024;
  private static final String TEST_KEY = "key";
  private static final String TEST_BUCKET = "bucket";
  private static final S3URI TEST_URI = S3URI.of(TEST_BUCKET, TEST_KEY);

  @BeforeEach
  void setup() throws URISyntaxException {
    // Generate random data
    Random r = new Random();
    byte[] data = new byte[OBJECT_SIZE];
    r.nextBytes(data);

    // Put random data in S3
    s3Client = createS3ClientV2(s3Mock.getHttpsEndpoint());
    s3Client
        .putObject(
            x -> x.bucket(TEST_URI.bucket()).key(TEST_URI.key()), AsyncRequestBody.fromBytes(data))
        .join();

    // Initialise streams
    objectClient = new S3SdkObjectClient(s3Client);
    s3SeekableInputStream = new S3SeekableInputStream(objectClient, TEST_URI);
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
    for (int i = 0, nextPos = 0; i < 100; nextPos = nextRandomPosition(r), ++i) {
      s3SeekableInputStream.seek(nextPos);
      inMemorySeekableStream.seek(nextPos);

      assertEquals(
          s3SeekableInputStream.getPos(),
          inMemorySeekableStream.getPos(),
          "positions should match");
      assertEquals(
          s3SeekableInputStream.read(),
          inMemorySeekableStream.read(),
          "returned data should match");
    }
  }

  private int nextRandomPosition(Random r) {
    return Math.abs(r.nextInt()) % OBJECT_SIZE;
  }
}
