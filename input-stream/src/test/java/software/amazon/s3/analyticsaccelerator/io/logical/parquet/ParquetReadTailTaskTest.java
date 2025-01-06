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
package software.amazon.s3.analyticsaccelerator.io.logical.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.concurrent.CompletionException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class ParquetReadTailTaskTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  void testConstructor() {
    assertNotNull(
        new ParquetReadTailTask(
            TEST_URI, Telemetry.NOOP, LogicalIOConfiguration.DEFAULT, mock(PhysicalIO.class)));
  }

  @Test
  void testConstructorFailsOnNull() {
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetReadTailTask(
                null, Telemetry.NOOP, LogicalIOConfiguration.DEFAULT, mock(PhysicalIO.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetReadTailTask(
                TEST_URI, null, LogicalIOConfiguration.DEFAULT, mock(PhysicalIO.class)));
    assertThrows(
        NullPointerException.class,
        () -> new ParquetReadTailTask(TEST_URI, Telemetry.NOOP, null, mock(PhysicalIO.class)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetReadTailTask(
                TEST_URI, Telemetry.NOOP, LogicalIOConfiguration.DEFAULT, null));
  }

  @Test
  void testTailRead() throws IOException {
    // Given: read tail task
    PhysicalIO mockedPhysicalIO = mock(PhysicalIO.class);
    when(mockedPhysicalIO.metadata())
        .thenReturn(ObjectMetadata.builder().contentLength(800).build());
    ParquetReadTailTask parquetReadTailTask =
        new ParquetReadTailTask(
            TEST_URI, Telemetry.NOOP, LogicalIOConfiguration.DEFAULT, mockedPhysicalIO);

    // When: file tail is requested
    FileTail fileTail = parquetReadTailTask.readFileTail();

    // Then: file tail is fetched from the PhysicalIO
    assertEquals(fileTail.getFileTailLength(), 800);
    verify(mockedPhysicalIO).readTail(any(byte[].class), anyInt(), anyInt(), any());
    verify(mockedPhysicalIO).metadata();
  }

  @Test
  @SneakyThrows
  void testExceptionRemappedToCompletionException() {
    // Given: read tail task with a throwing physicalIO under the hood
    PhysicalIO mockedPhysicalIO = mock(PhysicalIO.class);
    when(mockedPhysicalIO.metadata())
        .thenReturn(ObjectMetadata.builder().contentLength(800).build());
    when(mockedPhysicalIO.readTail(any(), anyInt(), anyInt(), any()))
        .thenThrow(new IOException("Something went horribly wrong."));
    ParquetReadTailTask parquetReadTailTask =
        new ParquetReadTailTask(
            TEST_URI, Telemetry.NOOP, LogicalIOConfiguration.DEFAULT, mockedPhysicalIO);

    // When & Then: file tail is requested --> IOException from PH/IO is wrapped in
    // CompletionException
    assertThrows(CompletionException.class, () -> parquetReadTailTask.readFileTail());
  }
}
