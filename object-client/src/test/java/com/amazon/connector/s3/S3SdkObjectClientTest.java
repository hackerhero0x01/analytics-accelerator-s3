package com.amazon.connector.s3;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.HeadRequest;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.request.ReadMode;
import com.amazon.connector.s3.request.Referrer;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.async.AbortableInputStreamSubscriber;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

public class S3SdkObjectClientTest {

  private static S3AsyncClient s3AsyncClient;

  @BeforeAll
  public static void setup() {
    s3AsyncClient = mock(S3AsyncClient.class);

    when(s3AsyncClient.headObject(any(HeadObjectRequest.class)))
        .thenReturn(
            CompletableFuture.completedFuture(
                HeadObjectResponse.builder().contentLength(42L).build()));

    when(s3AsyncClient.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
        .thenReturn(
            CompletableFuture.completedFuture(
                new ResponseInputStream<>(
                    GetObjectResponse.builder().build(),
                    AbortableInputStreamSubscriber.builder().build())));

    doNothing().when(s3AsyncClient).close();
  }

  @Test
  void testConstructorWithWrappedClient() {
    S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient);
    assertNotNull(client);
  }

  @Test
  void testConstructorWithConfiguration() {
    ObjectClientConfiguration configuration = ObjectClientConfiguration.DEFAULT;
    S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient, configuration);
    assertNotNull(client);
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new S3SdkObjectClient(null);
        });
  }

  @Test
  void testHeadObject() {
    S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient);
    assertEquals(
        client.headObject(HeadRequest.builder().bucket("bucket").key("key").build()).join(),
        ObjectMetadata.builder().contentLength(42).build());
  }

  @Test
  void testGetObjectWithRange() {
    S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient);
    assertInstanceOf(
        CompletableFuture.class,
        client.getObject(
            GetRequest.builder()
                .bucket("bucket")
                .key("key")
                .range(new Range(0, 20))
                .referrer(new Referrer("bytes=0-20", ReadMode.SYNC))
                .build()));
  }

  @Test
  void testObjectClientClose() {
    try (S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient)) {
      client.headObject(HeadRequest.builder().bucket("bucket").key("key").build());
    }
    verify(s3AsyncClient, times(1)).close();
  }
}
