package com.amazon.connector.s3.benchmark;

import com.amazon.connector.s3.S3SeekableInputStream;
import com.amazon.connector.s3.S3SeekableInputStreamFactory;
import com.amazon.connector.s3.datagen.BenchmarkData;
import com.amazon.connector.s3.datagen.BenchmarkData.Read;
import com.amazon.connector.s3.datagen.Constants;
import com.amazon.connector.s3.util.S3SeekableInputStreamBuilder;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
  private static final S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
      new S3SeekableInputStreamFactory(S3SeekableInputStreamBuilder.builder().build());

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

  /** Test backward seeks with S3 client */
  @Benchmark
  public void testBackwardSeeks__withStandardAsyncClient() {
    BenchmarkData.getBenchMarkObjectByName(key)
        .getBackwardSeekReadPattern()
        .forEach(range -> doReadWithAsyncClient(client, range));
  }

  /** Test backward seeks with SeekableStream */
  @Benchmark
  public void testBackwardSeeks__withSeekableStream() {
    S3SeekableInputStream stream = getStreamForKey(key);

    BenchmarkData.getBenchMarkObjectByName(key)
        .getBackwardSeekReadPattern()
        .forEach(range -> doReadWithStream(stream, range));
  }

  /** Test forward seeks with S3 client */
  @Benchmark
  public void testForwardSeeks__withStandardAsyncClient() {
    BenchmarkData.getBenchMarkObjectByName(key)
        .getForwardSeekReadPattern()
        .forEach(range -> doReadWithAsyncClient(client, range));
  }

  /** Test forward seeks with Seekable Stream */
  @Benchmark
  public void testForwardSeeks__withSeekableStream() {
    S3SeekableInputStream stream = getStreamForKey(key);

    BenchmarkData.getBenchMarkObjectByName(key)
        .getForwardSeekReadPattern()
        .forEach(range -> doReadWithStream(stream, range));
  }

  /** Test parquet-like reads with S3 client */
  @Benchmark
  public void testParquetLikeRead__withStandardAsyncClient() {
    BenchmarkData.getBenchMarkObjectByName(key)
        .getParquetLikeReadPattern()
        .forEach(range -> doReadWithAsyncClient(client, range));
  }

  /** Test parquet-like reads with Seekable Stream */
  @Benchmark
  public void testParquetLikeRead__withSeekableStream() {
    S3SeekableInputStream stream = getStreamForKey(key);

    BenchmarkData.getBenchMarkObjectByName(key)
        .getParquetLikeReadPattern()
        .forEach(range -> doReadWithStream(stream, range));
  }

  private void doReadWithAsyncClient(S3AsyncClient client, Read read) {
    CompletableFuture<ResponseInputStream<GetObjectResponse>> response =
        client.getObject(
            GetObjectRequest.builder()
                .bucket(Constants.BENCHMARK_BUCKET)
                .key(Constants.BENCHMARK_DATA_PREFIX_SEQUENTIAL + key)
                .range(rangeOf(read.getStart(), read.getStart() + read.getLength() - 1))
                .build(),
            AsyncResponseTransformer.toBlockingInputStream());

    try {
      System.out.println(IoUtils.toUtf8String(response.get()).hashCode());
    } catch (Exception e) {
      throw new RuntimeException("Could not finish read", e);
    }
  }

  private void doReadWithStream(S3SeekableInputStream stream, Read range) {
    try {
      stream.seek(range.getStart());

      int len = (int) range.getLength();
      byte[] buf = new byte[len];
      stream.read(buf, 0, len);
      String content = new String(buf, StandardCharsets.UTF_8);
      System.out.println(content.hashCode());
    } catch (IOException e) {
      new RuntimeException(
          String.format(
              "Could not fully read range %s-%s with SeekableStream",
              range.getStart(), range.getStart() + range.getLength()),
          e);
    }
  }

  private String rangeOf(long start, long end) {
    return String.format("bytes=%s-%s", start, end);
  }

  private S3SeekableInputStream getStreamForKey(String key) {
    return s3SeekableInputStreamFactory.createStream(
        S3URI.of(Constants.BENCHMARK_BUCKET, Constants.BENCHMARK_DATA_PREFIX_SEQUENTIAL + key));
  }
}
