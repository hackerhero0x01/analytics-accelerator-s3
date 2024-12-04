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

import lombok.Value;

/** Stores sizes to prefetch for file metadata and page index structures. */
@Value
public class FooterPrefetchSize {
  long fileMetadataPrefetchSize;
  long pageIndexPrefetchSize;

  /**
   * Default constructor to store footer prefetch sizes
   *
   * @param fileMetadataPrefetchSize size of file metadata to prefetch from file tail
   * @param pageIndexPrefetchSize size of page index to prefetch from file tail
   */
  public FooterPrefetchSize(long fileMetadataPrefetchSize, long pageIndexPrefetchSize) {
    this.fileMetadataPrefetchSize = fileMetadataPrefetchSize;
    this.pageIndexPrefetchSize = pageIndexPrefetchSize;
  }

  /**
   * Gets total size of data to be read from the tail of the file
   *
   * @return size of file tail read
   */
  public long getSize() {
    return fileMetadataPrefetchSize + pageIndexPrefetchSize;
  }
}
