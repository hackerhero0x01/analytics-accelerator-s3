package com.amazon.connector.s3;

import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.HeadRequest;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

/** Object client, based on AWS SDK v2 */
public class S3SdkObjectClient implements ObjectClient, AutoCloseable {

  public static final String HEADER_USER_AGENT = "User-Agent";

  private S3AsyncClient s3AsyncClient = null;
  private ObjectClientConfiguration objectClientConfiguration = null;
  private final UserAgent userAgent;

  /**
   * Create an instance of a S3 client, with default configuration, for interaction with Amazon S3
   * compatible object stores.
   *
   * @param s3AsyncClient Underlying client to be used for making requests to S3.
   */
  public S3SdkObjectClient(@NonNull S3AsyncClient s3AsyncClient) {
    this(s3AsyncClient, ObjectClientConfiguration.DEFAULT);
  }

  /**
   * Create an instance of a S3 client, for interaction with Amazon S3 compatible object stores.
   *
   * @param s3AsyncClient Underlying client to be used for making requests to S3.
   * @param objectClientConfiguration Configuration for object client.
   */
  public S3SdkObjectClient(
      @NonNull S3AsyncClient s3AsyncClient,
      @NonNull ObjectClientConfiguration objectClientConfiguration) {

    this.s3AsyncClient = s3AsyncClient;
    this.objectClientConfiguration = objectClientConfiguration;
    this.userAgent = new UserAgent();
    this.userAgent.prepend(objectClientConfiguration.getUserAgentPrefix());
  }

  @Override
  public void close() {
    s3AsyncClient.close();
  }

  /**
   * Make a headObject request to the object store.
   *
   * @param headRequest The HEAD request to be sent
   * @return HeadObjectResponse
   */
  @Override
  public CompletableFuture<ObjectMetadata> headObject(HeadRequest headRequest) {
    HeadObjectRequest.Builder builder =
        HeadObjectRequest.builder().bucket(headRequest.getBucket()).key(headRequest.getKey());

    // Add User-Agent header to the request.
    builder.overrideConfiguration(
        AwsRequestOverrideConfiguration.builder()
            .putHeader(HEADER_USER_AGENT, this.userAgent.getUserAgent())
            .build());

    return s3AsyncClient
        .headObject(builder.build())
        .thenApply(
            headObjectResponse ->
                ObjectMetadata.builder().contentLength(headObjectResponse.contentLength()).build());
  }

  /**
   * Make a getObject request to the object store.
   *
   * @param getRequest The GET request to be sent
   * @return ResponseInputStream<GetObjectResponse>
   */
  @Override
  public CompletableFuture<ObjectContent> getObject(GetRequest getRequest) {
    GetObjectRequest.Builder builder =
        GetObjectRequest.builder().bucket(getRequest.getBucket()).key(getRequest.getKey());

    if (Objects.nonNull(getRequest.getRange())) {
      String range = getRequest.getRange().toString();
      builder.range(range);

      builder.overrideConfiguration(
          AwsRequestOverrideConfiguration.builder()
              .putHeader("Referer", getRequest.getReferrer())
              .putHeader(HEADER_USER_AGENT, this.userAgent.getUserAgent())
              .build());
    }

    return s3AsyncClient
        .getObject(builder.build(), AsyncResponseTransformer.toBlockingInputStream())
        .thenApply(
            responseInputStream -> ObjectContent.builder().stream(responseInputStream).build());
  }
}
