package com.amazon.connector.s3.benchmarks;

import com.amazon.connector.s3.*;
import com.amazon.connector.s3.benchmarks.common.*;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.io.InputStream;
import lombok.NonNull;
import org.openjdk.jmh.annotations.*;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * Benchmarks which just read data sequentially. Useful for catching regressions in prefetching and
 * regressions in how much we utilise CRT.
 */
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public abstract class SequentialRead extends BenchmarkBase {
  private final BenchmarkObject benchmarkObject;

  /**
   * Creates a new instance of {@link SequentialRead} benchmark
   *
   * @param benchmarkObject and instance of {@link BenchmarkObject}
   */
  public SequentialRead(@NonNull BenchmarkObject benchmarkObject) {
    this.benchmarkObject = benchmarkObject;
  }
  /**
   * Runs the benchmark
   *
   * @throws IOException IO error, if encountered
   * @throws InterruptedException interrupted error, if encountered
   */
  @Benchmark
  public void run() throws IOException, InterruptedException {
    run(benchmarkObject);
  }

  /**
   * Sequentially reads a stream from S3 using an {@link S3AsyncClient}
   *
   * @param benchmarkObject {@link BenchmarkObject}
   * @throws IOException IO error, if encountered
   */
  @Override
  protected void processS3RawStream(BenchmarkObject benchmarkObject)
      throws IOException, InterruptedException {
    S3URI s3URI = benchmarkObject.getObjectUri(this.getBenchmarkContext());
    S3AsyncClient s3AsyncClient = this.getClient().getS3Client(this.getBenchmarkContext());
    try (InputStream inputStream =
        s3AsyncClient
            .getObject(
                GetObjectRequest.builder().bucket(s3URI.getBucket()).key(s3URI.getKey()).build(),
                AsyncResponseTransformer.toBlockingInputStream())
            .join()) {
      drainStream(inputStream, benchmarkObject, benchmarkObject.getSize());
    }
  }

  /**
   * Sequentially reads data from S3 using an {@link S3SeekableInputStream}
   *
   * @param benchmarkObject {@link BenchmarkObject}
   * @throws IOException IO error, if encountered
   */
  @Override
  public void processS3DATStream(BenchmarkObject benchmarkObject)
      throws IOException, InterruptedException {
    S3URI s3URI = benchmarkObject.getObjectUri(this.getBenchmarkContext());
    S3AsyncClient s3AsyncClient = this.getClient().getS3Client(this.getBenchmarkContext());

    try (S3SdkObjectClient sdkObjectClient = new S3SdkObjectClient(s3AsyncClient, false)) {
      try (S3SeekableInputStreamFactory factory =
          new S3SeekableInputStreamFactory(
              sdkObjectClient, S3SeekableInputStreamConfiguration.DEFAULT)) {
        try (InputStream inputStream = factory.createStream(s3URI)) {
          drainStream(inputStream, benchmarkObject, benchmarkObject.getSize());
        }
      }
    }
  }
}
