  /*
   * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
   *
   * Licensed under the Apache License, Version 2.0 (the "License").
   * You may not use this file except in compliance with the License.
   * You may obtain a copy of the License at
   *
   *   http://www.apache.org/licenses/LICENSE-2.0
  
   * Unless required by applicable law or agreed to in writing, software
   * distributed under the License is distributed on an "AS IS" BASIS,
   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   * See the License for the specific language governing permissions and
   * limitations under the License.
   */
package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.common.telemetry.Operation;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.util.S3URI;
import com.amazon.connector.s3.util.StreamAttributes;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task for reading the tail of a parquet file. */
public class ParquetReadTailTask {
  private final S3URI s3URI;
  private final Telemetry telemetry;
  private final LogicalIOConfiguration logicalIOConfiguration;
  private final PhysicalIO physicalIO;
  private static final String OPERATION_PARQUET_READ_TAIL = "parquet.task.read.tail";
  private static final Logger LOG = LoggerFactory.getLogger(ParquetReadTailTask.class);

  /**
   * Creates a new instance of {@link ParquetReadTailTask}.
   *
   * @param s3URI the S3URI of the object to read
   * @param telemetry an instance of {@link Telemetry} to use
   * @param logicalIOConfiguration LogicalIO configuration
   * @param physicalIO PhysicalIO instance
   */
  public ParquetReadTailTask(
      @NonNull S3URI s3URI,
      @NonNull Telemetry telemetry,
      @NonNull LogicalIOConfiguration logicalIOConfiguration,
      @NonNull PhysicalIO physicalIO) {
    this.s3URI = s3URI;
    this.telemetry = telemetry;
    this.logicalIOConfiguration = logicalIOConfiguration;
    this.physicalIO = physicalIO;
  }

  /**
   * Reads parquet file tail
   *
   * @return tail of parquet file
   */
  public FileTail readFileTail() {
    return telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_PARQUET_READ_TAIL)
                .attribute(StreamAttributes.uri(this.s3URI))
                .build(),
        () -> {
          long contentLength = physicalIO.metadata().getContentLength();
          Optional<Range> tailRangeOptional =
              ParquetUtils.getFileTailRange(logicalIOConfiguration, 0, contentLength);
          if (tailRangeOptional.isPresent()) {
            Range tailRange = tailRangeOptional.get();
            int tailLength = (int) tailRange.getLength();
            try {
              byte[] fileTail = new byte[tailLength];
              physicalIO.readTail(fileTail, 0, tailLength);
              return new FileTail(ByteBuffer.wrap(fileTail), (int) tailRange.getLength());
            } catch (Exception e) {
              LOG.error(
                  "Error in reading tail for {}. Will fallback to synchronous reading for this key.",
                  s3URI.getKey(),
                  e);
              throw new CompletionException("Error in getting file tail", e);
            }
          } else {
            // There's nothing to read, return an empty buffer
            return new FileTail(ByteBuffer.allocate(0), 0);
          }
        });
  }
}
