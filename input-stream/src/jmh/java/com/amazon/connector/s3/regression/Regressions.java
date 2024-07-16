package com.amazon.connector.s3.regression;

import com.amazon.connector.s3.S3SdkObjectClient;
import com.amazon.connector.s3.S3SeekableInputStream;
import com.amazon.connector.s3.S3SeekableInputStreamConfiguration;
import com.amazon.connector.s3.S3SeekableInputStreamFactory;
import com.amazon.connector.s3.datagen.Constants;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * In lieu of a better mechanism (as we don't have integration tests yet) I will put regression
 * tests here. These will run as part of micro-benchmarks, but we are only interested in them not
 * throwing.
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
@BenchmarkMode(Mode.SingleShotTime)
public class Regressions {

  @Benchmark
  public void testSequentialRead__withSeekableStream__icebergMetadataRegression()
      throws IOException {
    String key = "00000-36d34fef-1edb-44a2-9fef-80aceae9265a.metadata.json";

    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            new S3SdkObjectClient(S3AsyncClient.crtBuilder().maxConcurrency(300).build()),
            S3SeekableInputStreamConfiguration.DEFAULT);

    S3SeekableInputStream stream =
        s3SeekableInputStreamFactory.createStream(
            S3URI.of(Constants.BENCHMARK_BUCKET, Constants.BENCHMARK_DATA_PREFIX_SEQUENTIAL + key));

    byte[] b = new byte[4096];

    while (stream.read(b, 0, b.length) != -1) {}

    stream.close();
  }

  @Benchmark
  public void testSequentialRead__withSeekableStream__multiThreaded() throws IOException {
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            new S3SdkObjectClient(S3AsyncClient.crtBuilder().maxConcurrency(300).build()),
            S3SeekableInputStreamConfiguration.DEFAULT);

    LinkedList<CompletableFuture<Void>> futures = new LinkedList<>();

    for (int i = 1; i <= 8; ++i) {
      final String keyName = String.format("random-128mb-thread-%s.txt", i);
      futures.add(
          CompletableFuture.runAsync(
              () -> {
                try {
                  S3SeekableInputStream stream =
                      s3SeekableInputStreamFactory.createStream(
                          S3URI.of(
                              Constants.BENCHMARK_BUCKET,
                              Constants.BENCHMARK_DATA_PREFIX_SEQUENTIAL + keyName));

                  byte[] b = new byte[4096];

                  while (stream.read(b, 0, b.length) != -1) {}

                  stream.close();
                } catch (Throwable t) {
                  throw new RuntimeException(t);
                }
              }));
    }

    for (CompletableFuture<Void> future : futures) {
      future.join();
    }

    s3SeekableInputStreamFactory.close();
  }
}
