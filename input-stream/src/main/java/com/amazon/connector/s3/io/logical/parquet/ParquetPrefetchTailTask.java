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
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.util.S3URI;
import com.amazon.connector.s3.util.StreamAttributes;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Task for prefetching the tail of a parquet file. */
public class ParquetPrefetchTailTask {
  private final S3URI s3URI;
  private final Telemetry telemetry;
  private final LogicalIOConfiguration logicalIOConfiguration;
  private final PhysicalIO physicalIO;
  private static final String OPERATION_PARQUET_PREFETCH_TAIL = "parquet.task.prefetch.tail";
  private static final Logger LOG = LoggerFactory.getLogger(ParquetPrefetchTailTask.class);

  /**
   * Creates a new instance of {@link ParquetPrefetchTailTask}
   *
   * @param s3URI the S3URI of the object to prefetch
   * @param telemetry an instance of {@link Telemetry} to use
   * @param logicalIOConfiguration LogicalIO configuration
   * @param physicalIO PhysicalIO instance
   */
  public ParquetPrefetchTailTask(
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
   * Prefetch tail of the parquet file
   *
   * @return range of file prefetched
   */
  public List<Range> prefetchTail() {
    return telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_PARQUET_PREFETCH_TAIL)
                .attribute(StreamAttributes.uri(this.s3URI))
                .build(),
        () -> {
          try {
            long contentLength = physicalIO.metadata().getContentLength();
            Optional<Range> tailRangeOptional =
                ParquetUtils.getFileTailRange(logicalIOConfiguration, 0, contentLength);
            // Create a non-empty IOPlan only if we have a valid range to work with
            IOPlan ioPlan = tailRangeOptional.map(IOPlan::new).orElse(IOPlan.EMPTY_PLAN);
            physicalIO.execute(ioPlan);
            return ioPlan.getPrefetchRanges();
          } catch (Exception e) {
            LOG.error(
                "Error in executing tail prefetch plan for {}. Will fallback to reading footer synchronously.",
                this.s3URI.getKey(),
                e);
            throw new CompletionException("Error in executing tail prefetch plan", e);
          }
        });
  }
}
