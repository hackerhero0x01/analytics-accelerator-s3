package com.amazon.connector.s3.estimation;

import static com.amazon.connector.s3.util.Constants.ONE_KB;
import static com.amazon.connector.s3.util.Constants.ONE_MB;

import com.amazon.connector.s3.datagen.Constants;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * This class contains functions that allows us to make some order of magnitude estimates of S3
 * performance/behaviour.
 *
 * <p>These methods do not run as part of micro-benchmarks.
 */
public class OrderOfMagnitude {

  private static final String KEY = "random-1G.txt";

  @SneakyThrows
  public void testSingleThread() {
    S3AsyncClient client = S3AsyncClient.create();

    CompletableFuture<ResponseInputStream<GetObjectResponse>> response =
        client.getObject(
            GetObjectRequest.builder()
                .bucket(Constants.BENCHMARK_BUCKET)
                .key(Constants.BENCHMARK_DATA_PREFIX_SEQUENTIAL + KEY)
                .build(),
            AsyncResponseTransformer.toBlockingInputStream());

    consumeStream(response.join(), response.join().response().contentLength());
  }

  @SneakyThrows
  public void testMultipleThreads() {
    long size = 1024 * 1024 * 1024;
    long numThreads = 128;
    long rangeSize = size / numThreads;

    List<CompletableFuture<Long>> futureList = new LinkedList<>();
    for (int i = 0; i < numThreads; ++i) {
      futureList.add(fetchRange(crt, i * rangeSize, (i + 1) * rangeSize - 1));
    }

    for (int i = 0; i < numThreads; ++i) {
      futureList.get(i).join();
    }
  }

  private static final S3AsyncClient client = S3AsyncClient.create();
  private static final S3AsyncClient crt = S3AsyncClient.crtCreate();

  @SneakyThrows
  public void testReadsOfDifferentSizes__withAsyncClient() {
    long size = 64 * ONE_KB;
    fetchRange(client, 0, size - 1).join();
  }

  @SneakyThrows
  public void testReadsOfDifferentSizes__withCrt() {
    long size = 8 * ONE_MB;
    fetchRange(crt, 0, size - 1).join();
  }

  private CompletableFuture<Long> fetchRange(S3AsyncClient client, long start, long end) {
    return CompletableFuture.supplyAsync(
        () -> {
          CompletableFuture<ResponseInputStream<GetObjectResponse>> response =
              client.getObject(
                  GetObjectRequest.builder()
                      .bucket(Constants.BENCHMARK_BUCKET)
                      .key(Constants.BENCHMARK_DATA_PREFIX_SEQUENTIAL + "random-1G.txt")
                      .range(String.format("bytes=%s-%s", start, end))
                      .build(),
                  AsyncResponseTransformer.toBlockingInputStream());

          return consumeStream(response.join(), response.join().response().contentLength());
        });
  }

  @SneakyThrows
  private long consumeStream(InputStream inputStream, long contentLength) {
    long numBytesRemaining = contentLength;
    long numBytesRead = 0;

    byte[] buffer = new byte[64 * ONE_KB];

    while (numBytesRead < numBytesRemaining) {
      int r = inputStream.read(buffer);

      if (r == -1) {
        throw new RuntimeException("unexpected EOF");
      }

      numBytesRead += r;
    }

    return numBytesRead;
  }
}
