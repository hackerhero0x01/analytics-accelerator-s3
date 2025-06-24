/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.benchmarks.data.generation;

import java.io.File;
import lombok.NonNull;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.s3.analyticsaccelerator.access.S3ExecutionContext;
import software.amazon.s3.analyticsaccelerator.access.S3ObjectKind;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * Uploads parquet files present in the resources folder to S3 to allow running of tests involving
 * parquet data . It uses {@link S3TransferManager} backed by the CRT to upload data.
 */
public class ParquetObjectGenerator extends BenchmarkObjectGenerator {

  /**
   * Creates an instance of parquet object generator
   *
   * @param context an instance of {@link S3ExecutionContext}
   * @param kind S3 Object Kind
   */
  public ParquetObjectGenerator(@NonNull S3ExecutionContext context, @NonNull S3ObjectKind kind) {
    super(context, kind);
  }

  @Override
  public void generate(S3URI s3URI, long size) {
    String fileName = s3URI.getKey().substring(s3URI.getKey().lastIndexOf('/') + 1);
    File file = new File("input-stream/src/jmh/resources/" + fileName);
    String progressPrefix = "[" + s3URI + "] ";
    System.out.println(progressPrefix + "Starting upload from: " + file.getAbsolutePath());

    try (S3TransferManager s3TransferManager =
        S3TransferManager.builder().s3Client(this.getContext().getS3CrtClient()).build()) {

      PutObjectRequest.Builder requestBuilder =
          PutObjectRequest.builder()
              .bucket(s3URI.getBucket())
              .key(s3URI.getKey())
              .contentType("application/x-parquet");

      if (getKind().equals(S3ObjectKind.RANDOM_PARQUET_ENCRYPTED) && CUSTOMER_KEY != null) {
        requestBuilder
            .sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
            .sseCustomerKey(CUSTOMER_KEY)
            .sseCustomerKeyMD5(calculateBase64MD5());
      }

      // Use uploadFile instead of upload with InputStream
      UploadFileRequest uploadFileRequest =
          UploadFileRequest.builder()
              .putObjectRequest(requestBuilder.build())
              .source(file.toPath())
              .build();

      System.out.println(progressPrefix + "Uploading");
      CompletedFileUpload completedUpload =
          s3TransferManager.uploadFile(uploadFileRequest).completionFuture().join();
      System.out.println(progressPrefix + "Done");

      // Verify the upload
      System.out.println(progressPrefix + "Verifying data...");

      HeadObjectRequest.Builder headRequestBuilder =
          HeadObjectRequest.builder().bucket(s3URI.getBucket()).key(s3URI.getKey());

      if (getKind().equals(S3ObjectKind.RANDOM_PARQUET_ENCRYPTED) && CUSTOMER_KEY != null) {
        headRequestBuilder
            .sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
            .sseCustomerKey(CUSTOMER_KEY)
            .sseCustomerKeyMD5(calculateBase64MD5());
      }

      HeadObjectResponse headObjectResponse =
          this.getContext().getS3CrtClient().headObject(headRequestBuilder.build()).join();

      if (!completedUpload.response().eTag().equals(headObjectResponse.eTag())) {
        throw new IllegalStateException(
            progressPrefix
                + "Expected eTag: "
                + completedUpload.response().eTag()
                + "; actual eTag: "
                + headObjectResponse.eTag());
      }
      System.out.println(progressPrefix + "Done");

    } catch (RuntimeException e) {
      throw new RuntimeException("Failed to upload file", e);
    }
  }
}
