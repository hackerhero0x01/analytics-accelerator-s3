package com.amazon.connector.s3;

import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.HeadRequest;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

/** Object client, based on AWS SDK v2 */
public class S3SdkObjectClient implements ObjectClient, AutoCloseable {

  private final S3AsyncClient s3AsyncClient;

  /**
   * Create an instance of a S3 client for interaction with Amazon S3 compatible object stores.
   *
   * @param s3AsyncClient Underlying client to be used for making requests to S3. Useful for
   *     applications that need to configure the S3 Client. If no client is provided, an instance of
   *     the S3 CRT client is created.
   */
  public S3SdkObjectClient(S3AsyncClient s3AsyncClient) {

    if (s3AsyncClient != null) {
      this.s3AsyncClient = s3AsyncClient;
    } else {
      this.s3AsyncClient = S3AsyncClient.crtBuilder().build();
    }
  }

  @Override
  public void close() {
    s3AsyncClient.close();
  }

  @Override
  public CompletableFuture<ObjectMetadata> headObject(HeadRequest headRequest) {
    return s3AsyncClient
        .headObject(
            HeadObjectRequest.builder()
                .bucket(headRequest.getBucket())
                .key(headRequest.getKey())
                .build())
        .thenApply(
            headObjectResponse ->
                ObjectMetadata.builder().contentLength(headObjectResponse.contentLength()).build());
  }

  @Override
  public CompletableFuture<ObjectContent> getObject(GetRequest getRequest) {
    GetObjectRequest.Builder builder =
        GetObjectRequest.builder().bucket(getRequest.getBucket()).key(getRequest.getKey());

    if (Objects.nonNull(getRequest.getRange())) {
      builder.range(
          String.format(
              "bytes=%s-%s", getRequest.getRange().getStart(), getRequest.getRange().getEnd()));
    }

    return s3AsyncClient
        .getObject(builder.build(), AsyncResponseTransformer.toBlockingInputStream())
        .thenApply(
            responseInputStream -> ObjectContent.builder().stream(responseInputStream).build());
  }
}
