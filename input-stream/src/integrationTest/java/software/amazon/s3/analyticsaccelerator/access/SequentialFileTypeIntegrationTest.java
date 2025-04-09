/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.util.ObjectFormat;
import software.amazon.s3.analyticsaccelerator.util.ObjectFormatSelector;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

class SequentialFileTypeIntegrationTest extends IntegrationTestBase {

  @ParameterizedTest
  @MethodSource("sequentialReader")
  void testingSequentialReads(
      S3ClientKind clientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration)
      throws IOException {
    testAndCompareStreamReadPattern(clientKind, s3Object, streamReadPattern, configuration);
  }

  static Stream<Arguments> sequentialReader() {
    return argumentsFor(
        getS3ClientKinds(),
        S3Object.getSequentialS3Objects(),
        sequentialPatterns(),
        getS3SeekableInputStreamConfigurations());
  }

  @Test
  void testSequentialFilesUseSequentialImplementation() {
    List<S3Object> sequentialFiles = S3Object.getSequentialS3Objects();

    ObjectFormatSelector formatSelector = new ObjectFormatSelector(LogicalIOConfiguration.DEFAULT);

    for (S3Object sequentialFile : sequentialFiles) {
      S3URI s3URI =
          sequentialFile.getObjectUri(this.getS3ExecutionContext().getConfiguration().getBaseUri());

      ObjectFormat selectedFormat =
          formatSelector.getObjectFormat(s3URI, OpenStreamInformation.DEFAULT);

      assertEquals(
          ObjectFormat.SEQUENTIAL,
          selectedFormat,
          String.format(
              "File %s should be handled by Sequential implementation", sequentialFile.getName()));
    }
  }

  @Test
  void testInvalidPartitionSizeConfiguration() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("logicalio.partition.size", "-1"); // negative partition size

    ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration(configMap, "");

    assertThrows(
        IllegalArgumentException.class,
        () -> S3SeekableInputStreamConfiguration.fromConfiguration(connectorConfiguration));
  }

  @Test
  void testSequentialPrefetchSize() throws IOException, InterruptedException {
    long partitionSize = 16 * 1024 * 1024;

    Map<String, String> configMap = new HashMap<>();
    configMap.put("logicalio.partition.size", String.valueOf(partitionSize));
    ConnectorConfiguration connectorConfiguration = new ConnectorConfiguration(configMap, "");

    S3Object largeFile = S3Object.CSV_20MB;
    verifyPrefetchBehavior(largeFile, partitionSize, connectorConfiguration);
  }

  private void verifyPrefetchBehavior(
      S3Object file, long partitionSize, ConnectorConfiguration connectorConfiguration)
      throws IOException, InterruptedException {
    S3URI s3URI = file.getObjectUri(this.getS3ExecutionContext().getConfiguration().getBaseUri());

    S3SeekableInputStreamFactory factory =
        new S3SeekableInputStreamFactory(
            this.getS3ExecutionContext().getObjectClient(),
            S3SeekableInputStreamConfiguration.fromConfiguration(connectorConfiguration));

    try (S3SeekableInputStream stream = factory.createStream(s3URI)) {
      byte[] singleByte = new byte[1];
      int firstByteRead = stream.read(singleByte);
      assertTrue(firstByteRead > 0, "Should be able to read first byte");

      // Brief pause to allow prefetch to start
      Thread.sleep(100);

      long expectedPrefetchSize = Math.min(file.getSize(), partitionSize);

      // Read up to the expected prefetch size
      byte[] buffer = new byte[8192];
      long bytesReadInPrefetch = 1; // Start with 1 for the first byte we already read
      int bytesRead;

      while (bytesReadInPrefetch < expectedPrefetchSize
          && (bytesRead =
                  stream.read(
                      buffer,
                      0,
                      (int) Math.min(buffer.length, expectedPrefetchSize - bytesReadInPrefetch)))
              != -1) {
        bytesReadInPrefetch += bytesRead;
      }

      // Verify we could read the expected prefetch amount
      assertEquals(
          expectedPrefetchSize,
          bytesReadInPrefetch,
          String.format(
              "Should read %d bytes from prefetch for %s", expectedPrefetchSize, file.getName()));
    }
  }
}
