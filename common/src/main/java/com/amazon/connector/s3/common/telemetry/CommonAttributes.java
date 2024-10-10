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
package com.amazon.connector.s3.common.telemetry;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** Common telemetry attributes. */
@Getter
@AllArgsConstructor
public enum CommonAttributes {
  THREAD_ID("thread_id");
  private final String name;

  /**
   * Creates a and {@link Attribute} for a {@link CommonAttributes#THREAD_ID}.
   *
   * @param thread the {@link CommonAttributes#THREAD_ID} to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute threadId(Thread thread) {
    return Attribute.of(CommonAttributes.THREAD_ID.getName(), thread.getId());
  }
}
