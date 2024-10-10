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
package com.amazon.connector.s3.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class PrefetchModeTest {

  @Test
  public void testPrefetchModeFromString() {
    assertEquals(PrefetchMode.ALL, PrefetchMode.fromString("aLL"));
    assertEquals(PrefetchMode.OFF, PrefetchMode.fromString("OFF"));
    assertEquals(PrefetchMode.COLUMN_BOUND, PrefetchMode.fromString("column_bound"));
    assertEquals(PrefetchMode.ROW_GROUP, PrefetchMode.fromString("row_group"));

    // defaults to ROW_GROUP mode
    assertEquals(PrefetchMode.ROW_GROUP, PrefetchMode.fromString("xyz"));
  }
}
