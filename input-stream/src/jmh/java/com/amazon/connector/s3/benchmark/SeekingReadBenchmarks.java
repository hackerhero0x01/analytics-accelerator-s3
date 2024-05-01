package com.amazon.connector.s3.benchmark;

import com.amazon.connector.s3.datagen.BenchmarkData;
import com.amazon.connector.s3.datagen.BenchmarkData.Read;
import com.amazon.connector.s3.datagen.Constants;
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
 * Benchmarks following a read pattern which jumps around in the stream. This is useful to catch
 * regressions in column-oriented read patterns. We also have tests for backwards seeks.
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.SingleShotTime)
public class SeekingReadBenchmarks {

  private static final S3AsyncClient client = S3AsyncClient.create();

  @Param(value = {"random-1mb.txt", "random-4mb.txt", "random-16mb.txt"})
  private String key;

  /** Test backwards seek */
  @Benchmark
  public void testBackwardsSeeks__withStandardAsyncClient() {
    BenchmarkData.getBenchMarkObjectByName(key)
        .getBackwardSeekReadPattern()
        .forEach(range -> doReadWithAsyncClient(client, range));
  }

  /** Test forwards seek */
  @Benchmark
  public void testForwardSeeks__withStandardAsyncClient() {
    BenchmarkData.getBenchMarkObjectByName(key)
        .getForwardSeekReadPattern()
        .forEach(range -> doReadWithAsyncClient(client, range));
  }

  /** Test parquet-like reads */
  @Benchmark
  public void testParquetLikeRead__withStandardAsyncClient() {
    BenchmarkData.getBenchMarkObjectByName(key)
        .getParquetLikeReadPattern()
        .forEach(range -> doReadWithAsyncClient(client, range));
  }

  private void doReadWithAsyncClient(S3AsyncClient client, Read read) {
    CompletableFuture<ResponseInputStream<GetObjectResponse>> response =
        client.getObject(
            GetObjectRequest.builder()
                .bucket(Constants.BENCHMARK_BUCKET)
                .key(Constants.BENCHMARK_DATA_PREFIX_SEQUENTIAL + key)
                .range(rangeOf(read.getStart(), read.getStart() + read.getLength()))
                .build(),
            AsyncResponseTransformer.toBlockingInputStream());

    try {
      IoUtils.toUtf8String(response.get());
    } catch (Exception e) {
      throw new RuntimeException("Could not finish read", e);
    }
  }

  private String rangeOf(long start, long end) {
    return String.format("bytes=%s-%s", start, end);
  }
}
