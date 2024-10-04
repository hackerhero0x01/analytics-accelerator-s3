package com.amazon.connector.s3.benchmarks;

import com.amazon.connector.s3.S3SdkObjectClient;
import com.amazon.connector.s3.S3SeekableInputStreamConfiguration;
import com.amazon.connector.s3.S3SeekableInputStreamFactory;
import com.amazon.connector.s3.benchmarks.common.BenchmarkObject;
import com.amazon.connector.s3.benchmarks.common.StreamRead;
import com.amazon.connector.s3.benchmarks.common.StreamReadPattern;
import com.amazon.connector.s3.benchmarks.common.StreamReadPatternKind;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import org.openjdk.jmh.annotations.*;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * Benchmarks following a read pattern which jumps around in the stream. This is useful to catch
 * regressions in column-oriented read patterns. We also have tests for backwards seeks.
 */
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public abstract class SeekingRead extends BenchmarkBase {
  private final BenchmarkObject benchmarkObject;
  private final AtomicReference<StreamReadPattern> streamReadPattern = new AtomicReference<>();

  /**
   * Creates a new instance of {@link SequentialRead} benchmark
   *
   * @param benchmarkObject and instance of {@link BenchmarkObject}
   */
  public SeekingRead(@NonNull BenchmarkObject benchmarkObject) {
    this.benchmarkObject = benchmarkObject;
  }

  /**
   * Runs the `SKIPPING_FORWARD` IO
   *
   * @throws IOException IO error thrown
   * @throws InterruptedException interrupted error thrown
   */
  @Benchmark
  public void forward() throws IOException, InterruptedException {
    this.streamReadPattern.set(
        StreamReadPatternKind.SKIPPING_FORWARD.getStreamReadPattern(benchmarkObject));
    run(benchmarkObject);
  }

  /**
   * Runs the `SKIPPING_BACKWARD` IO
   *
   * @throws IOException IO error thrown
   * @throws InterruptedException interrupted error thrown
   */
  @Benchmark
  public void backward() throws IOException, InterruptedException {
    this.streamReadPattern.set(
        StreamReadPatternKind.SKIPPING_BACKWARD.getStreamReadPattern(benchmarkObject));
    run(benchmarkObject);
  }

  /**
   * Runs the `QUASI_PARQUET_SIMPLE` IO
   *
   * @throws IOException IO error thrown
   * @throws InterruptedException interrupted error thrown
   */
  @Benchmark
  public void quasiSimpleParquet() throws IOException, InterruptedException {
    this.streamReadPattern.set(
        StreamReadPatternKind.QUASI_PARQUET_SIMPLE.getStreamReadPattern(benchmarkObject));
    run(benchmarkObject);
  }

  /**
   * Processes series of seeks using the {@link S3AsyncClient}
   *
   * @param benchmarkObject {@link BenchmarkObject}
   * @throws IOException IO error thrown
   * @throws InterruptedException interrupted error thrown
   */
  @Override
  protected void processS3RawStream(BenchmarkObject benchmarkObject)
      throws IOException, InterruptedException {
    StreamReadPattern streamReadPattern = this.streamReadPattern.get();

    // Replay the pattern through series of GETs
    S3URI s3URI = benchmarkObject.getObjectUri(this.getBenchmarkContext());
    S3AsyncClient s3AsyncClient = this.getClient().getS3Client(this.getBenchmarkContext());
    for (StreamRead streamRead : streamReadPattern.getStreamReads()) {
      // Issue a ranged GET and get InputStream
      InputStream inputStream =
          s3AsyncClient
              .getObject(
                  GetObjectRequest.builder()
                      .bucket(s3URI.getBucket())
                      .key(s3URI.getKey())
                      .range(
                          String.format(
                              "bytes=%s-%s",
                              streamRead.getStart(),
                              streamRead.getStart() + streamRead.getLength() - 1))
                      .build(),
                  AsyncResponseTransformer.toBlockingInputStream())
              .join();
      // drain  bytes
      drainStream(inputStream, benchmarkObject, streamRead.getLength());
    }
  }

  /**
   * Processes series of seeks using the {@link com.amazon.connector.s3.S3SeekableInputStream}
   *
   * @param benchmarkObject {@link BenchmarkObject}
   * @throws IOException IO error thrown
   * @throws InterruptedException interrupted error thrown
   */
  @Override
  public void processS3DATStream(BenchmarkObject benchmarkObject)
      throws IOException, InterruptedException {
    StreamReadPattern streamReadPattern = this.streamReadPattern.get();
    // Replay the pattern through series of seeks and drains
    S3URI s3URI = benchmarkObject.getObjectUri(this.getBenchmarkContext());
    S3AsyncClient s3AsyncClient = this.getClient().getS3Client(this.getBenchmarkContext());

    // Create the factory and the streams
    try (S3SdkObjectClient sdkObjectClient = new S3SdkObjectClient(s3AsyncClient, false)) {
      try (S3SeekableInputStreamFactory factory =
          new S3SeekableInputStreamFactory(
              sdkObjectClient, S3SeekableInputStreamConfiguration.DEFAULT)) {
        try (InputStream inputStream = factory.createStream(s3URI)) {
          // Apply seeks
          for (StreamRead streamRead : streamReadPattern.getStreamReads()) {
            // drain bytes
            drainStream(inputStream, benchmarkObject, streamRead.getLength());
          }
        }
      }
    }
  }
}
