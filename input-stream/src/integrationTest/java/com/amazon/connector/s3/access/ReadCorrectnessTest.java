package com.amazon.connector.s3.access;

import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests read correctness on multiple sizes and read patterns */
public class ReadCorrectnessTest extends IntegrationTestBase {
  @ParameterizedTest
  @MethodSource("sequentialReads")
  void testSequentialReads(
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      DATInputStreamConfigurationKind configuration)
      throws IOException {
    testAndCompareStreamReadPattern(s3Object, streamReadPattern, configuration);
  }

  @ParameterizedTest
  @MethodSource("skippingReads")
  void testSkippingReads(
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      DATInputStreamConfigurationKind configuration)
      throws IOException {
    testAndCompareStreamReadPattern(s3Object, streamReadPattern, configuration);
  }

  @ParameterizedTest
  @MethodSource("parquetReads")
  void testQuasiParquetReads(
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      DATInputStreamConfigurationKind configuration)
      throws IOException {
    testAndCompareStreamReadPattern(s3Object, streamReadPattern, configuration);
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
