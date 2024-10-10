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
package software.amazon.s3.dataaccelerator.io.logical.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static software.amazon.s3.dataaccelerator.util.Constants.ONE_MB;

import org.junit.jupiter.api.Test;
import software.amazon.s3.dataaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.dataaccelerator.request.Range;

public class ParquetUtilsTest {
  @Test
  void testGetFileTailRangeDefaultConfig() {

    Range range =
        ParquetUtils.getFileTailRange(LogicalIOConfiguration.DEFAULT, 0, 5 * ONE_MB).get();

    assertEquals(
        range.getStart(), 5 * ONE_MB - LogicalIOConfiguration.DEFAULT.getFooterCachingSize());
    assertEquals(range.getEnd(), 5 * ONE_MB - 1);
  }

  @Test
  void testGetFileTailRangeSmallFile() {
    Range range =
        ParquetUtils.getFileTailRange(
                LogicalIOConfiguration.builder()
                    .smallObjectsPrefetchingEnabled(true)
                    .smallObjectSizeThreshold(2 * ONE_MB)
                    .build(),
                0,
                2 * ONE_MB)
            .get();

    assertEquals(range.getStart(), 0);
    assertEquals(range.getEnd(), 2 * ONE_MB - 1);
  }

  @Test
  void testGetFileTailSmallContentLength() {

    Range range = ParquetUtils.getFileTailRange(LogicalIOConfiguration.DEFAULT, 0, 5).get();

    assertEquals(range.getStart(), 0);
    assertEquals(range.getEnd(), 4);
  }
}
