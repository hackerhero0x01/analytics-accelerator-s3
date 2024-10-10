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
package software.amazon.s3.dataaccelerator.util;

import java.net.URI;
import lombok.NonNull;
import lombok.Value;

/** Container for representing an 's3://' or 's3a://'-style S3 location. */
@Value(staticConstructor = "of")
public class S3URI {
  @NonNull String bucket;
  @NonNull String key;

  private static String URI_FORMAT_STRING = "%s://%s/%s";
  private static String URI_SCHEME_DEFAULT = "s3";

  /**
   * Creates the {@link URI} corresponding to this {@link S3URI}.
   *
   * @return the newly created {@link S3URI}
   */
  public URI toURI() {
    return toURI(URI_SCHEME_DEFAULT);
  }

  /**
   * Creates the {@link URI} corresponding to this {@link S3URI}.
   *
   * @param scheme URI scheme to use
   * @return the newly created {@link S3URI}
   */
  public URI toURI(String scheme) {
    return URI.create(this.toString(scheme));
  }

  /**
   * Creates the string representation of the {@link S3URI}.
   *
   * @return the string representation of the {@link URI}
   */
  @Override
  public String toString() {
    return toString(URI_SCHEME_DEFAULT);
  }

  /**
   * Creates the string representation of the {@link S3URI}.
   *
   * @param scheme URI scheme to use
   * @return the string representation of the {@link URI}
   */
  public String toString(@NonNull String scheme) {
    return String.format(URI_FORMAT_STRING, scheme, this.getBucket(), this.getKey());
  }
}
