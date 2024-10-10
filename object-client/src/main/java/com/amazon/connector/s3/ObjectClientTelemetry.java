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
package com.amazon.connector.s3;

import com.amazon.connector.s3.common.telemetry.Attribute;
import com.amazon.connector.s3.util.S3URI;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** Helper class to streamline Telemetry calls. */
@Getter
@AllArgsConstructor
enum ObjectClientTelemetry {
  URI("uri"),
  RANGE("range");
  private final String name;

  public static final String OPERATION_GET = "s3.client.get";
  public static final String OPERATION_HEAD = "s3.client.head";

  /**
   * Creates an {@link Attribute} for a {@link S3URI}.
   *
   * @param s3URI the {@link S3URI} to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute uri(S3URI s3URI) {
    return Attribute.of(ObjectClientTelemetry.URI.getName(), s3URI.toString());
  }
}
