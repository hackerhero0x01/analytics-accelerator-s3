package com.amazon.connector.s3.benchmarks.data.generation;

import com.amazon.connector.s3.benchmarks.common.BenchmarkContext;
import com.amazon.connector.s3.util.S3URI;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import lombok.NonNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;

/** Generates random data with a given size */
public class RandomSequentialObjectGenerator extends BenchmarkObjectGenerator {
  /**
   * Creates an instance of random data generator
   *
   * @param context an instance of {@link BenchmarkContext}
   */
  public RandomSequentialObjectGenerator(@NonNull BenchmarkContext context) {
    super(context, BenchmarkObjectGeneratorKind.RANDOM_SEQUENTIAL);
  }

  /**
   * Generate data
   *
   * @param s3URI S3 URI to generate data into
   * @param size object size
   */
  @Override
  public void generate(S3URI s3URI, long size) {
    int bufferSize =
        (int)
            Math.min(
                size,
                getContext()
                    .getConfiguration()
                    .getClientFactoryConfiguration()
                    .getCrtPartSizeInBytes());
    String progressPrefix = "[" + s3URI + "] ";
    System.out.println(
        progressPrefix + "Generating with " + size + " bytes [" + bufferSize + " bytes buffer]");
    try (S3TransferManager s3TransferManager =
        S3TransferManager.builder().s3Client(this.getContext().getS3CrtClient()).build()) {
      UploadRequest uploadRequest =
          UploadRequest.builder()
              .putObjectRequest(
                  PutObjectRequest.builder().bucket(s3URI.getBucket()).key(s3URI.getKey()).build())
              .requestBody(
                  AsyncRequestBody.fromPublisher(
                      new RandomDataGeneratorPublisher(size, bufferSize)))
              .build();
      System.out.println(progressPrefix + "Uploading");
      CompletedUpload completedUpload =
          s3TransferManager.upload(uploadRequest).completionFuture().join();
      System.out.println(progressPrefix + "Done");

      System.out.println(progressPrefix + "Verifying data...");
      HeadObjectResponse headObjectResponse =
          this.getContext()
              .getS3CrtClient()
              .headObject(
                  HeadObjectRequest.builder().bucket(s3URI.getBucket()).key(s3URI.getKey()).build())
              .join();
      System.out.println(progressPrefix + "Done");
      if (headObjectResponse.contentLength() != size) {
        throw new IllegalStateException(
            progressPrefix
                + "Expected object size: "
                + size
                + "; actual object size: "
                + headObjectResponse.contentLength());
      }

      if (!completedUpload.response().eTag().equals(headObjectResponse.eTag())) {
        throw new IllegalStateException(
            progressPrefix
                + "Expected eTag: "
                + completedUpload.response().eTag()
                + "; actual eTag: "
                + headObjectResponse.eTag());
      }
    }
  }

  /** The publisher that produces random data */
  static class RandomDataGeneratorPublisher implements Publisher<ByteBuffer> {
    private final int bufferSize;
    private final AtomicLong bytesRemaining;

    /**
     * Creates a new instance of {@link RandomSequentialObjectGenerator}
     *
     * @param size object size
     * @param bufferSize buffer size
     */
    RandomDataGeneratorPublisher(long size, int bufferSize) {
      this.bufferSize = bufferSize;
      this.bytesRemaining = new AtomicLong(size);
    }
    /**
     * Called by the {@link S3TransferManager} to subscribe to new data
     *
     * @param s the {@link Subscriber} that will consume signals from this {@link Publisher}
     */
    @Override
    public void subscribe(@NonNull Subscriber<? super ByteBuffer> s) {
      s.onSubscribe(
          new Subscription() {
            @Override
            public void request(long n) {
              // Report progress
              long remaining = bytesRemaining.addAndGet(-1 * bufferSize);
              // Figure out how much we should fetch and whether we should complete
              int currentBufferSize = bufferSize;
              boolean completed = false;
              if (remaining < 0) {
                completed = true;
                remaining = -1 * remaining;
                // we are buffer size past the end, this means a concurrent fetch completed this
                if (remaining >= bufferSize) {
                  remaining = -1;
                }
                currentBufferSize = (int) remaining;
              }

              // Allocate data and return it
              if (currentBufferSize > 0) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(currentBufferSize);
                ThreadLocalRandom.current().nextBytes(byteBuffer.array());
                s.onNext(byteBuffer);
              }

              // Completed if we are done
              if (completed) {
                s.onComplete();
              }
            }

            @Override
            public void cancel() {}
          });
    }
  }
}
