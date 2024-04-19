package com.amazon.connector.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

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
  public HeadObjectResponse headObject(HeadObjectRequest headObjectRequest) {
    return s3AsyncClient.headObject(headObjectRequest).join();
  }

  @Override
  public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest getObjectRequest) {
    return s3AsyncClient
        .getObject(getObjectRequest, AsyncResponseTransformer.toBlockingInputStream())
        .join();
  }
}
