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
package software.amazon.s3.analyticsaccelerator.request;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;

/** Wrapper class around HeadObjectResponse abstracting away from S3-specific details */
@Data
@Builder
public class ObjectMetadata {
  /**
   * The length of the object content in bytes. This value must be non-negative.
   *
   * @param contentLength the content length to set
   * @return the builder instance
   * @throws IllegalArgumentException if contentLength is negative
   */
  long contentLength;

  /**
   * The entity tag of the object.
   *
   * @param etag the etag to set
   * @return the builder instance
   * @throws NullPointerException if etag is null
   */
  @NonNull String etag;

  @Builder
  private ObjectMetadata(long contentLength, @NonNull String etag) {
    Preconditions.checkArgument(contentLength >= 0, "content length must be non-negative");

    this.contentLength = contentLength;
    this.etag = etag;
  }
}
