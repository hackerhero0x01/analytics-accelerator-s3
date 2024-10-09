package com.amazon.connector.s3.access;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * This tests concurrency and thread safety of teh shared state. While the DAT InputStream itself is
 * not thread-safe, the shared state that multiple streams access and manipulate should be.
 */
public class ConcurrencyCorrectnessTest extends IntegrationTestBase {
  private static final int CONCURRENCY_LEVEL = 3;
  private static final int CONCURRENCY_ITERATIONS = 2;

  @ParameterizedTest
  @MethodSource("sequentialReads")
  void testSequentialReads(
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      DATInputStreamConfigurationKind configuration)
      throws IOException, InterruptedException, ExecutionException {
    testDATReadConcurrency(
        s3Object, streamReadPattern, configuration, CONCURRENCY_LEVEL, CONCURRENCY_ITERATIONS);
  }

  @ParameterizedTest
  @MethodSource("skippingReads")
  void testSkippingReads(
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      DATInputStreamConfigurationKind configuration)
      throws IOException, InterruptedException, ExecutionException {
    testDATReadConcurrency(
        s3Object, streamReadPattern, configuration, CONCURRENCY_LEVEL, CONCURRENCY_ITERATIONS);
  }

  @ParameterizedTest
  @MethodSource("parquetReads")
  void testQuasiParquetReads(
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      DATInputStreamConfigurationKind configuration)
      throws IOException, InterruptedException, ExecutionException {
    testDATReadConcurrency(
        s3Object, streamReadPattern, configuration, CONCURRENCY_LEVEL, CONCURRENCY_ITERATIONS);
  }

  static Stream<Arguments> sequentialReads() {
    return argumentsFor(
        S3Object.smallAndMediumObjects(),
        sequentialPatterns(),
        getS3SeekableInputStreamConfigurations());
  }

  static Stream<Arguments> skippingReads() {
    return argumentsFor(
        S3Object.smallAndMediumObjects(),
        skippingPatterns(),
        getS3SeekableInputStreamConfigurations());
  }

  static Stream<Arguments> parquetReads() {
    return argumentsFor(
        S3Object.smallAndMediumObjects(),
        parquetPatterns(),
        getS3SeekableInputStreamConfigurations());
  }
}
