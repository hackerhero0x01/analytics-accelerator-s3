package com.amazon.connector.s3.benchmarks.data.generation;

import com.amazon.connector.s3.benchmarks.common.BenchmarkConfiguration;
import com.amazon.connector.s3.benchmarks.common.BenchmarkContext;
import com.amazon.connector.s3.benchmarks.common.BenchmarkObject;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;

/**
 * This class implements data generation for the sequential read micro-benchmarks. This allows for
 * deterministic data generation which in turn allows to run reproducible micro-benchmarks. The
 * results of microbenchmarks are not to be compared across different computers (Mac of engineer A
 * and DevDesk of engineer B will have different results), but runs on the same computer should be
 * comparable.
 */
public class BenchmarkDataGeneratorDriver {
  /**
   * Entry point: set bucket name and prefix in {@link Constants} to generate a dataset of random
   * objects
   *
   * @param args program arguments are currently ignored
   */
  public static void main(String[] args) throws IOException {
    System.out.println("Starting data generation...");
    try (BenchmarkContext benchmarkContext =
        new BenchmarkContext(BenchmarkConfiguration.fromEnvironment())) {
      // Output the configuration
      System.out.println(benchmarkContext.getConfiguration());

      // For each object, generate the data
      for (BenchmarkObject benchmarkObject : BenchmarkObject.values()) {
        // Build the Url
        S3URI s3URI = benchmarkObject.getObjectUri(benchmarkContext);

        // Create the generator
        BenchmarkObjectGenerator benchmarkObjectGenerator =
            benchmarkObject.getKind().createGenerator(benchmarkContext);

        // Generate the data
        benchmarkObjectGenerator.generate(s3URI, benchmarkObject.getSize());
      }
    }
  }
}
