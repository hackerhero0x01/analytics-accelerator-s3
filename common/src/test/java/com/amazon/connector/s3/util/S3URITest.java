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
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.net.URI;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class S3URITest {
  @Test
  void testCreateValidatesArguments() {
    assertThrows(NullPointerException.class, () -> S3URI.of(null, "key"));
    assertThrows(NullPointerException.class, () -> S3URI.of("bucket", null));
  }

  @Test
  void testCreate() {
    S3URI s3URI = S3URI.of("bucket", "key");

    assertEquals("bucket", s3URI.getBucket());
    assertEquals("key", s3URI.getKey());
  }

  @Test
  void testToString() {
    S3URI s3URI = S3URI.of("bucket", "key");
    assertEquals("s3://bucket/key", s3URI.toString());
    assertEquals("s3a://bucket/key", s3URI.toString("s3a"));
    assertThrows(NullPointerException.class, () -> s3URI.toString(null));
  }

  @Test
  void testToUri() {
    S3URI s3URI = S3URI.of("bucket", "key");
    assertEquals(URI.create("s3://bucket/key"), s3URI.toURI());
    assertEquals(URI.create("s3a://bucket/key"), s3URI.toURI("s3a"));
    assertThrows(NullPointerException.class, () -> s3URI.toURI(null));
  }
}
