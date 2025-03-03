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
package software.amazon.s3.analyticsaccelerator.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class defining prefetch modes. */
public enum PrefetchMode {
  OFF("off"),
  ALL("all"),
  ROW_GROUP("row_group"),
  COLUMN_BOUND("column_bound");

  private final String name;

  private static final Logger LOG = LoggerFactory.getLogger(PrefetchMode.class);

  PrefetchMode(String name) {
    this.name = name;
  }

  /**
   * Converts user supplied configuration to enum. Defaults to ROW_GROUP is user input is not
   * recognised.
   *
   * @param prefetchMode user supplier prefetch mode
   * @return PrefetchMode enum to use
   */
  public static PrefetchMode fromString(String prefetchMode) {
    for (PrefetchMode value : values()) {
      if (value.name.equalsIgnoreCase(prefetchMode)) {
        return value;
      }
    }
    LOG.debug("Unknown prefetch mode {}, using default row_group mode.", prefetchMode);

    return ROW_GROUP;
  }
}
