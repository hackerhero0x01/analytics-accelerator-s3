package com.amazon.connector.s3.datagen;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Random;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * This class implements data generation for the sequential read micro-benchmarks. This allows for
 * deterministic data generation which in turn allows to run reproducible micro-benchmarks. The
 * results of microbenchmarks are not to be compared across different computers (Mac of engineer A
 * and DevDesk of engineer B will have different results), but runs on the same computer should be
 * comparable.
 */
public class SequentialReadDataGenerator {

  private static final int ONE_MB_IN_BYTES = 1 * 1024 * 1024;

  /** (object-name, size) pairs */
  private static final Map<String, Integer> OBJECTS =
      ImmutableMap.of(
          "random-1mb.txt", ONE_MB_IN_BYTES,
          "random-4mb.txt", 4 * ONE_MB_IN_BYTES,
          "random-16mb.txt", 16 * ONE_MB_IN_BYTES,
          "random-64mb.txt", 64 * ONE_MB_IN_BYTES,
          "random-128mb.txt", 128 * ONE_MB_IN_BYTES,
          "random-256mb.txt", 256 * ONE_MB_IN_BYTES);

  /**
   * Entry point: set bucket name and prefix in {@link Constants} to generate a dataset of random
   * objects
   */
  public static void main(String[] args) {
    OBJECTS.forEach(SequentialReadDataGenerator::generateObject);
  }

  private static void generateObject(String key, Integer size) {
    String fullKeyName =
        String.format(
            "s3://%s/%s",
            Constants.BENCHMARK_BUCKET, Constants.BENCHMARK_DATA_PREFIX_SEQUENTIAL + key);
    System.out.println("Generating " + fullKeyName + " and uploading it to S3...");

    S3AsyncClient s3AsyncClient = S3AsyncClient.create();
    s3AsyncClient
        .putObject(
            PutObjectRequest.builder()
                .bucket(Constants.BENCHMARK_BUCKET)
                .key(Constants.BENCHMARK_DATA_PREFIX_SEQUENTIAL + key)
                .build(),
            AsyncRequestBody.fromBytes(generateBytes(size)))
        .join();
  }

  private static byte[] generateBytes(int len) {
    byte[] buf = new byte[len];
    Random random = new Random();
    random.nextBytes(buf);
    return buf;
  }

  /**
   * PRBS31-style pseudo-random number generator for producing deterministic 'random' data of given
   * size. https://en.wikipedia.org/wiki/Pseudorandom_binary_sequence
   */
  private static int prbs31(int state) {
    int feedback = ((state >> 30) ^ (state >> 27)) & 1;
    return ((state << 1) | feedback) & 0xffffffff;
  }
}
