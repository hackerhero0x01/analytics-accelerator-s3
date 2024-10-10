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
package com.amazon.connector.s3.util;

import com.amazon.connector.s3.common.telemetry.Attribute;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.request.Range;
import lombok.AllArgsConstructor;
import lombok.Getter;

/** Helper class to streamline Telemetry calls. */
@Getter
@AllArgsConstructor
public enum StreamAttributes {
  URI("uri"),
  RANGE("range"),
  VARIANT("variant"),
  EFFECTIVE_RANGE("range.effective"),
  GENERATION("generation"),
  COLUMN("column"),
  IOPLAN("ioplan");
  private final String name;

  /**
   * Creates an {@link Attribute} for a {@link S3URI}.
   *
   * @param s3URI the {@link S3URI} to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute uri(S3URI s3URI) {
    return Attribute.of(StreamAttributes.URI.getName(), s3URI.toString());
  }

  /**
   * Creates an {@link Attribute} for a {@link S3URI}.
   *
   * @param variant the variant of the operation
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute variant(String variant) {
    return Attribute.of(StreamAttributes.VARIANT.getName(), variant);
  }

  /**
   * Creates an {@link Attribute} for a {@link Range}.
   *
   * @param start range start.
   * @param end range end.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute range(long start, long end) {
    return range(new Range(start, end));
  }

  /**
   * Creates an {@link Attribute} for a {@link Range}.
   *
   * @param range range.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute range(Range range) {
    return Attribute.of(StreamAttributes.RANGE.getName(), range.toString());
  }

  /**
   * Creates an {@link Attribute} for an effective {@link Range}.
   *
   * @param start range start.
   * @param end range end.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute effectiveRange(long start, long end) {
    return effectiveRange(new Range(start, end));
  }

  /**
   * Creates an {@link Attribute} for anm affective {@link Range}.
   *
   * @param range range.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute effectiveRange(Range range) {
    return Attribute.of(StreamAttributes.EFFECTIVE_RANGE.getName(), range.toString());
  }

  /**
   * Creates an {@link Attribute} for generation.
   *
   * @param generation the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute generation(long generation) {
    return Attribute.of(StreamAttributes.GENERATION.getName(), generation);
  }

  /**
   * Creates an {@link Attribute} for generation.
   *
   * @param column the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute column(String column) {
    return Attribute.of(StreamAttributes.COLUMN.getName(), column);
  }

  /**
   * Creates an {@link Attribute} for ioPlan.
   *
   * @param ioPlan the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute ioPlan(IOPlan ioPlan) {
    return Attribute.of(StreamAttributes.IOPLAN.getName(), ioPlan.toString());
  }
}
