package com.amazon.connector.s3.benchmarks.data.generation;

import com.amazon.connector.s3.access.S3ExecutionConfiguration;
import com.amazon.connector.s3.access.S3ExecutionContext;
import com.amazon.connector.s3.access.S3Object;
import com.amazon.connector.s3.access.S3ObjectKind;
import com.amazon.connector.s3.util.S3URI;
import java.io.IOException;

/**
 * This class implements data generation for the sequential read micro-benchmarks. This allows for
 * deterministic data generation which in turn allows to run reproducible micro-benchmarks. The
 * results of micro benchmarks are not to be compared across different computers (Mac of engineer A
 * and DevDesk of engineer B will have different results), but runs on the same computer should be
 * comparable.
 */
public class BenchmarkDataGeneratorDriver {
  /**
   * This is the entry point to the generation code. ALl the context is extracted from the
   * Environment variables via {@link S3ExecutionConfiguration#fromEnvironment()}. The list of
   * relevant variable can be found in both {@link S3ExecutionConfiguration} and {@link
   * com.amazon.connector.s3.access.S3AsyncClientFactoryConfiguration}
   *
   * <p>The mandatory variables are:
   *
   * <ul>
   *   <li>`S3_TEST_BUCKET` - the bucket to generate data into
   *   <li>`S3_TEST_PREFIX` - the root prefix to generate data into
   *   <li>`S3_TEST_REGION` - the region the bucket is in
   * </ul>
   *
   * <p>The rest allows to fine tune the CRT, buffer sizes and so on and are set to sensible
   * defaults.
   *
   * @param args program arguments are currently ignored
   */
  public static void main(String[] args) throws IOException {
    System.out.println("Starting data generation...");
    try (S3ExecutionContext s3ExecutionContext =
        new S3ExecutionContext(S3ExecutionConfiguration.fromEnvironment())) {
      // Output the configuration
      System.out.println(s3ExecutionContext.getConfiguration());

      // For each object, generate the data
      for (S3Object s3Object : S3Object.values()) {
        // Build the Url
        S3URI s3URI = s3Object.getObjectUri(s3ExecutionContext.getConfiguration().getBaseUri());

        // Create the generator
        BenchmarkObjectGenerator benchmarkObjectGenerator =
            createGenerator(s3ExecutionContext, s3Object.getKind());

        // Generate the data
        benchmarkObjectGenerator.generate(s3URI, s3Object.getSize());
      }
    }
  }

  /**
   * Creates a generator for a given context and generator kind
   *
   * @param context generator context
   * @param s3ObjectKind generator s3ObjectKind S3 Object Kind
   * @return a new instance of {@link BenchmarkObjectGenerator}
   */
  private static BenchmarkObjectGenerator createGenerator(
      S3ExecutionContext context, S3ObjectKind s3ObjectKind) {
    switch (s3ObjectKind) {
      case RANDOM_SEQUENTIAL:
        return new RandomSequentialObjectGenerator(context);
      default:
        throw new IllegalArgumentException("Unsupported kind: " + s3ObjectKind);
    }
  }
}
