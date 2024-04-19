package com.amazon.connector.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * Represents APIs of an Amazon S3 compatible object store
 */
public interface ObjectClient {

  /**
   * Make a headObject request to S3.
   *
   * @param headObjectRequest HeadObjectRequest
   * @return HeadObjectResponse
   */
  HeadObjectResponse headObject(HeadObjectRequest headObjectRequest);

  /**
   * Make a getObject request to S3.
   *
   * @param getObjectRequest GetObjectRequest
   * @return ResponseInputStream<GetObjectResponse>
   */
  ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest getObjectRequest);

}

