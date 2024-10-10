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
package com.amazon.connector.s3.benchmarks;

import com.amazon.connector.s3.access.StreamReadPatternKind;

/**
 * Container for the benchmarks based on {@link StreamPatternComparisonBenchmark} * We create
 * separate classes for each type of read pattern so that we get separate reports for each type of
 * pattern
 */
public class StreamPatternComparisonBenchmarks {
  /** SEQUENTIAL pattern */
  public static class ComparisonSequential extends StreamPatternComparisonBenchmark {
    /** Constructor */
    public ComparisonSequential() {
      super(StreamReadPatternKind.SEQUENTIAL);
    }
  }

  /** SKIPPING_FORWARD pattern */
  public static class ComparisonSkippingForward extends StreamPatternComparisonBenchmark {
    /** Constructor */
    public ComparisonSkippingForward() {
      super(StreamReadPatternKind.SKIPPING_FORWARD);
    }
  }

  /** SKIPPING_BACKWARD pattern */
  public static class ComparisonSkippingBackward extends StreamPatternComparisonBenchmark {
    /** Constructor */
    public ComparisonSkippingBackward() {
      super(StreamReadPatternKind.SKIPPING_BACKWARD);
    }
  }

  /** QUASI_PARQUET_ROW_GROUP pattern */
  public static class ComparisonQuasiParquetRowGroup extends StreamPatternComparisonBenchmark {
    /** Constructor */
    public ComparisonQuasiParquetRowGroup() {
      super(StreamReadPatternKind.QUASI_PARQUET_ROW_GROUP);
    }
  }

  /** QUASI_PARQUET_COLUMN_CHUNK */
  public static class ComparisonQuasiParquetColumnChunk extends StreamPatternComparisonBenchmark {
    /** Constructor */
    public ComparisonQuasiParquetColumnChunk() {
      super(StreamReadPatternKind.QUASI_PARQUET_COLUMN_CHUNK);
    }
  }
}
