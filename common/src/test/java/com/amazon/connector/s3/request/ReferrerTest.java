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
package com.amazon.connector.s3.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

public class ReferrerTest {

  @Test
  void testConstructor() {
    Referrer referrer = new Referrer(null, ReadMode.SYNC);
    assertNotNull(referrer);
  }

  @Test
  void testReferrerToString() {
    Referrer referrer = new Referrer("bytes=11083511-19472118", ReadMode.ASYNC);
    assertEquals(referrer.toString(), "bytes=11083511-19472118,readMode=ASYNC");
  }
}
