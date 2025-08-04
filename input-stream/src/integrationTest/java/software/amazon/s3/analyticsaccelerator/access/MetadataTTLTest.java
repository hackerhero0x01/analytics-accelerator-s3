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

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.*;
import lombok.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class MetadataTTLTest extends IntegrationTestBase {

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testStreamConsistencySmallObjectAfterEtagChange(S3ClientKind s3ClientKind)
      throws IOException, InterruptedException {
    testEtagBehaviorAfterObjectChange(s3ClientKind, S3Object.RANDOM_4MB, false);
  }

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testStreamConsistencyLargeObjectEtagChange(S3ClientKind s3ClientKind)
      throws IOException, InterruptedException {
    testEtagBehaviorAfterObjectChange(s3ClientKind, S3Object.RANDOM_16MB, true);
  }

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testNewStreamGetsUpdatedVersionAfterTtl(S3ClientKind s3ClientKind)
      throws IOException, InterruptedException {
    testNewStreamDetectsObjectUpdateAfterTtlExpiry(s3ClientKind, S3Object.RANDOM_1MB);
  }

  /**
   * Test that existing stream maintains etag consistency even after metadata TTL expires, serve
   * stale bytes if present in cache, if not then throw 412 Small objects (<8MB) are fully
   * prefetched on first read, so all data remains cached and accessible even after etag changes,
   * avoiding 412 errors.
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object S3 object to read
   * @param shouldThrow412 whether the test should expect a 412
   */
  protected void testEtagBehaviorAfterObjectChange(
      @NonNull S3ClientKind s3ClientKind, @NonNull S3Object s3Object, boolean shouldThrow412)
      throws IOException, InterruptedException {

    Map<String, String> configMap = new HashMap<>();
    configMap.put("metadata.cache.ttl", "100");
    ConnectorConfiguration config = new ConnectorConfiguration(configMap, "");
    S3SeekableInputStreamConfiguration streamConfig =
        S3SeekableInputStreamConfiguration.fromConfiguration(config);

    S3URI s3URI =
        s3Object.getObjectUri(this.getS3ExecutionContext().getConfiguration().getBaseUri());
    S3AsyncClient s3Client = this.getS3ExecutionContext().getS3Client();

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, streamConfig)) {

      S3SeekableInputStream stream1 =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT);

      byte[] data1 = new byte[1000];
      int bytesRead1 = stream1.read(data1);
      assertTrue(bytesRead1 > 0, "Should read some data");

      Thread.sleep(150); // Wait for metadata TTL expiry

      byte[] newData = generateRandomBytes((int) s3Object.getSize());
      s3Client
          .putObject(
              x -> x.bucket(s3URI.getBucket()).key(s3URI.getKey()),
              AsyncRequestBody.fromBytes(newData))
          .join();

      long seekPosition = s3Object.getSize() - 1000;
      stream1.seek(seekPosition);

      if (shouldThrow412) {
        // Large objects: should throw 412 when accessing uncached blocks
        IOException ex =
            assertThrows(
                IOException.class,
                () -> {
                  byte[] data2 = new byte[1000];
                  int bytesRead2 = stream1.read(data2);
                  assertTrue(bytesRead2 > 0, "Should read data before exception");
                });
        Throwable cause = ex.getCause();
        assertTrue(cause instanceof S3Exception, "Cause should be S3Exception");
        S3Exception s3Exception = (S3Exception) cause;
        assertEquals(412, s3Exception.statusCode());
      } else {
        byte[] data2 = new byte[1000];
        int bytesRead = stream1.read(data2);
        assertTrue(bytesRead > 0, "Should read from prefetched cache without 412 error");
      }

      stream1.close();
    }
  }

  /**
   * Test that new streams detect object updates and serves new version of data after metadata TTL
   * expires
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object S3 object to read
   */
  protected void testNewStreamDetectsObjectUpdateAfterTtlExpiry(
      @NonNull S3ClientKind s3ClientKind, @NonNull S3Object s3Object)
      throws IOException, InterruptedException {

    Map<String, String> configMap = new HashMap<>();
    configMap.put("metadatastore.ttl", "50");
    ConnectorConfiguration config = new ConnectorConfiguration(configMap, "");
    S3SeekableInputStreamConfiguration streamConfig =
        S3SeekableInputStreamConfiguration.fromConfiguration(config);

    S3URI s3URI =
        s3Object.getObjectUri(this.getS3ExecutionContext().getConfiguration().getBaseUri());
    S3AsyncClient s3Client = this.getS3ExecutionContext().getS3Client();
    int bufferSize = (int) s3Object.getSize();

    try (S3AALClientStreamReader s3AALClientStreamReader =
        this.createS3AALClientStreamReader(s3ClientKind, streamConfig)) {

      S3SeekableInputStream stream1 =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT);
      Crc32CChecksum originalChecksum = calculateCRC32C(stream1, bufferSize);
      stream1.close();

      Thread.sleep(100);

      // Update the object(change Etag)
      byte[] newData = generateRandomBytes(bufferSize);
      s3Client
          .putObject(
              x -> x.bucket(s3URI.getBucket()).key(s3URI.getKey()),
              AsyncRequestBody.fromBytes(newData))
          .join();

      S3SeekableInputStream stream2 =
          s3AALClientStreamReader.createReadStream(s3Object, OpenStreamInformation.DEFAULT);
      Crc32CChecksum updatedChecksum = calculateCRC32C(stream2, bufferSize);
      stream2.close();

      // Checksums should be different (object was updated)
      assertNotEquals(
          originalChecksum.getChecksumBytes(),
          updatedChecksum.getChecksumBytes(),
          "Checksums should differ after object update");
    }
  }
}
