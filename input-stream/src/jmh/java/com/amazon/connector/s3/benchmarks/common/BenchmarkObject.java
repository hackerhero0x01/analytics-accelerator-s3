package com.amazon.connector.s3.benchmarks.common;

import static com.amazon.connector.s3.benchmarks.data.generation.Constants.ONE_GB_IN_BYTES;
import static com.amazon.connector.s3.benchmarks.data.generation.Constants.ONE_MB_IN_BYTES;

import com.amazon.connector.s3.benchmarks.data.generation.BenchmarkObjectGeneratorKind;
import com.amazon.connector.s3.util.S3URI;
import java.util.Locale;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

/** Represents a singular object to run benchmarks against */
@AllArgsConstructor
@Getter
public enum BenchmarkObject {
  RANDOM_1MB("random-1mb.bin", 1 * ONE_MB_IN_BYTES, BenchmarkObjectGeneratorKind.RANDOM_SEQUENTIAL),
  RANDOM_4MB("random-4mb.bin", 4 * ONE_MB_IN_BYTES, BenchmarkObjectGeneratorKind.RANDOM_SEQUENTIAL),
  RANDOM_16MB(
      "random-16mb.bin", 16 * ONE_MB_IN_BYTES, BenchmarkObjectGeneratorKind.RANDOM_SEQUENTIAL),
  RANDOM_64MB(
      "random-64mb.bin", 64 * ONE_MB_IN_BYTES, BenchmarkObjectGeneratorKind.RANDOM_SEQUENTIAL),
  RANDOM_128MB(
      "random-128mb.bin", 128 * ONE_MB_IN_BYTES, BenchmarkObjectGeneratorKind.RANDOM_SEQUENTIAL),
  RANDOM_256MB(
      "random-256mb.bin", 256 * ONE_MB_IN_BYTES, BenchmarkObjectGeneratorKind.RANDOM_SEQUENTIAL),
  RANDOM_512MB(
      "random-512mb.bin", 512 * ONE_MB_IN_BYTES, BenchmarkObjectGeneratorKind.RANDOM_SEQUENTIAL),
  RANDOM_1G("random-1G.bin", ONE_GB_IN_BYTES, BenchmarkObjectGeneratorKind.RANDOM_SEQUENTIAL),
  RANDOM_5G("random-5G.bin", 5L * ONE_GB_IN_BYTES, BenchmarkObjectGeneratorKind.RANDOM_SEQUENTIAL),
  RANDOM_10G(
      "random-10G.bin", 10L * ONE_GB_IN_BYTES, BenchmarkObjectGeneratorKind.RANDOM_SEQUENTIAL);

  private final String name;
  private final long size;
  private final BenchmarkObjectGeneratorKind kind;

  /**
   * Get S3 Object Uri based on the content
   *
   * @param benchmarkContext an instance of {@link BenchmarkContext}
   * @return {@link S3URI}
   */
  public S3URI getObjectUri(@NonNull BenchmarkContext benchmarkContext) {
    return S3URI.of(
        benchmarkContext.getConfiguration().getBucket(),
        benchmarkContext.getConfiguration().getPrefix()
            + "/"
            + this.getKind().getValue().toLowerCase(Locale.getDefault())
            + "/"
            + this.getName());
  }
}
