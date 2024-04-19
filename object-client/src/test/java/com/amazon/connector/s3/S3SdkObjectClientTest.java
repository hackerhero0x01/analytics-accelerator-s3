package com.amazon.connector.s3;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
        .thenReturn(CompletableFuture.completedFuture(HeadObjectResponse.builder().build()));

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
  void testConstructorWithDefaultClient() {
    S3SdkObjectClient client = new S3SdkObjectClient(null);
    assertNotNull(client);
  }

  @Test
  void testHeadObject() {
    S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient);
    assertEquals(
        client.headObject(HeadObjectRequest.builder().build()),
        HeadObjectResponse.builder().build());
  }

  @Test
  void testGetObject() {
    S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient);
    assertInstanceOf(
        ResponseInputStream.class, client.getObject(GetObjectRequest.builder().build()));
  }

  @Test
  void testObjectClientClose() {
    try (S3SdkObjectClient client = new S3SdkObjectClient(s3AsyncClient)) {
      client.headObject(HeadObjectRequest.builder().build());
    }
    verify(s3AsyncClient, times(1)).close();
  }
}
