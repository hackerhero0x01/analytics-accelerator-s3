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

import java.io.IOException;
import java.util.Optional;
import lombok.NonNull;
import software.amazon.awssdk.core.checksums.Crc32CChecksum;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;

/**
 * This class is a base for performance (JMH) and integration tests and contains common
 * functionality to create and drain streams, replay read patterns etc
 */
public abstract class ExecutionBase {
  /**
   * Returns the current {@link S3ExecutionContext}
   *
   * @return {@link S3ExecutionContext}
   */
  protected abstract S3ExecutionContext getS3ExecutionContext();

  /**
   * Creates an instance of {@link S3AsyncClientStreamReader} that uses {@link
   * software.amazon.awssdk.services.s3.S3AsyncClient} to read from S3
   *
   * @param s3ClientKind S3 Client kind
   * @return an instance of {@link S3AsyncClientStreamReader}
   */
  protected S3AsyncClientStreamReader createS3AsyncClientStreamReader(
      @NonNull S3ClientKind s3ClientKind) {
    return new S3AsyncClientStreamReader(
        s3ClientKind.getS3Client(this.getS3ExecutionContext()),
        this.getS3ExecutionContext().getConfiguration().getBaseUri(),
        this.getS3ExecutionContext().getConfiguration().getBufferSizeBytes());
  }

  /**
   * Creates an instance of {@link S3DATClientStreamReader} that uses DAT to read from S3
   *
   * @param s3ClientKind S3 Client kind
   * @param DATInputStreamConfigurationKind {@link S3SeekableInputStreamConfiguration} kind
   * @return an instance of {@link S3DATClientStreamReader}
   */
  protected S3DATClientStreamReader createS3DATClientStreamReader(
      @NonNull S3ClientKind s3ClientKind,
      @NonNull DATInputStreamConfigurationKind DATInputStreamConfigurationKind) {
    return new S3DATClientStreamReader(
        s3ClientKind.getS3Client(this.getS3ExecutionContext()),
        DATInputStreamConfigurationKind.getValue(),
        this.getS3ExecutionContext().getConfiguration().getBaseUri(),
        this.getS3ExecutionContext().getConfiguration().getBufferSizeBytes());
  }

  /**
   * Executes a pattern directly on an S3 Client.
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object {@link } S3 Object to run the pattern on
   * @param streamReadPattern the read pattern
   * @param checksum checksum to update, if specified
   * @throws IOException IO error, if thrown
   */
  protected void executeReadPatternDirectly(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPattern streamReadPattern,
      Optional<Crc32CChecksum> checksum)
      throws IOException {
    try (S3AsyncClientStreamReader s3AsyncClientStreamReader =
        this.createS3AsyncClientStreamReader(s3ClientKind)) {
      s3AsyncClientStreamReader.readPattern(s3Object, streamReadPattern, checksum);
    }
  }

  /**
   * Executes a pattern on DAT
   *
   * @param s3ClientKind S3 client kind to use
   * @param s3Object {@link S3Object} S3 Object to run the pattern on
   * @param DATInputStreamConfigurationKind DAT configuration
   * @param streamReadPattern the read pattern
   * @param checksum checksum to update, if specified
   * @throws IOException IO error, if thrown
   */
  protected void executeReadPatternOnDAT(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPattern streamReadPattern,
      DATInputStreamConfigurationKind DATInputStreamConfigurationKind,
      Optional<Crc32CChecksum> checksum)
      throws IOException {
    try (S3DATClientStreamReader s3DATClientStreamReader =
        this.createS3DATClientStreamReader(s3ClientKind, DATInputStreamConfigurationKind)) {
      executeReadPatternOnDAT(s3Object, s3DATClientStreamReader, streamReadPattern, checksum);
    }
  }

  /**
   * Executes a pattern on DAT
   *
   * @param s3Object {@link S3Object} S3 Object to run the pattern on
   * @param s3DATClientStreamReader DAT stream reader
   * @param streamReadPattern the read pattern
   * @param checksum checksum to update, if specified
   * @throws IOException IO error, if thrown
   */
  protected void executeReadPatternOnDAT(
      S3Object s3Object,
      S3DATClientStreamReader s3DATClientStreamReader,
      StreamReadPattern streamReadPattern,
      Optional<Crc32CChecksum> checksum)
      throws IOException {
    s3DATClientStreamReader.readPattern(s3Object, streamReadPattern, checksum);
  }
}
