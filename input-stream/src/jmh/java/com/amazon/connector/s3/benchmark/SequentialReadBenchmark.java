package com.amazon.connector.s3.benchmark;

import com.amazon.connector.s3.S3SdkObjectClient;
import com.amazon.connector.s3.S3SeekableInputStream;
import com.amazon.connector.s3.S3SeekableInputStreamConfiguration;
import com.amazon.connector.s3.S3SeekableInputStreamFactory;
import com.amazon.connector.s3.datagen.Constants;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.utils.IoUtils;

/**
 * Benchmarks which just read data sequentially. Useful for catching regressions in prefetching and
 * regressions in how much we utilise CRT.
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.SingleShotTime)
public class SequentialReadBenchmark {

  private static final S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
      new S3SeekableInputStreamFactory(
          new S3SdkObjectClient(S3AsyncClient.crtBuilder().maxConcurrency(300).build()),
          S3SeekableInputStreamConfiguration.DEFAULT);

  @Param(
      value = {
        "random-1mb.txt",
        "random-4mb.txt",
        "random-16mb.txt",
        // TODO: Extend this parameter to bigger objects once we improve performance
        // https://app.asana.com/0/1206885953994785/1207212328457565/f
        // "random-64mb.txt",
        // "random-128mb.txt",
        // "random-256mb.txt"
      })
  private String key;

  /**
   * Not a perfect baseline but will do for now. Use the standard S3Async client for sequential
   * reads and compare its performance to seekable stream.
   */
  @Benchmark
  public void testSequentialRead__withStandardAsyncClient() throws IOException {
    S3AsyncClient client = S3AsyncClient.create();
    CompletableFuture<ResponseInputStream<GetObjectResponse>> response =
        client.getObject(
            GetObjectRequest.builder()
                .bucket(Constants.BENCHMARK_BUCKET)
                .key(Constants.BENCHMARK_DATA_PREFIX_SEQUENTIAL + key)
                .build(),
            AsyncResponseTransformer.toBlockingInputStream());

    String content = IoUtils.toUtf8String(response.join());
    System.out.println(content.hashCode());
  }

  /** Test sequential reads with seekable streams. */
  @Benchmark
  public void testSequentialRead__withSeekableStream() throws IOException {
    S3SeekableInputStream stream =
        s3SeekableInputStreamFactory.createStream(
            S3URI.of(Constants.BENCHMARK_BUCKET, Constants.BENCHMARK_DATA_PREFIX_SEQUENTIAL + key));

    String content = IoUtils.toUtf8String(stream);
    System.out.println(content.hashCode());
  }
}
