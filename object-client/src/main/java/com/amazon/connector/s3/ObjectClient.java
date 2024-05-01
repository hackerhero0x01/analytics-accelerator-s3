package com.amazon.connector.s3;

import com.amazon.connector.s3.object.ObjectContent;
import com.amazon.connector.s3.object.ObjectMetadata;
import com.amazon.connector.s3.request.GetRequest;
import com.amazon.connector.s3.request.HeadRequest;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.core.async.ResponsePublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/** Represents APIs of an Amazon S3 compatible object store */
public interface ObjectClient extends Closeable {

  /**
   * Make a headObject request to the object store.
   *
   * @param headRequest The HEAD request to be sent
   * @return HeadObjectResponse
   */
  CompletableFuture<ObjectMetadata> headObject(HeadRequest headRequest);

  /**
   * Make a getObject request to the object store.
   *
   * @param getRequest The GET request to be sent
   * @return ResponseInputStream<GetObjectResponse>
   */
  CompletableFuture<ObjectContent> getObject(GetRequest getRequest);


  CompletableFuture<ResponsePublisher<GetObjectResponse>> getObject2(GetRequest getRequest);
