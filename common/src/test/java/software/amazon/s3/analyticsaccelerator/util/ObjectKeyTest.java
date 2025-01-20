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

import static org.junit.jupiter.api.Assertions.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class ObjectKeyTest {

  @Test
  void testMapOperations() {
    // Create test objects
    S3URI s3uri1 = S3URI.of("bucket", "key");
    S3URI s3uri2 = S3URI.of("bucket", "key");
    S3URI s3uri3 = S3URI.of("bucket", "key2");

    ObjectKey key1 = ObjectKey.builder().s3URI(s3uri1).etag("etag1").build();

    ObjectKey key2 = ObjectKey.builder().s3URI(s3uri2).etag("etag1").build();

    ObjectKey key3 = ObjectKey.builder().s3URI(s3uri1).etag("etag2").build();

    ObjectKey key4 = ObjectKey.builder().s3URI(s3uri3).etag("etag1").build();

    Map<ObjectKey, String> map = new HashMap<>();

    // Test putting and retrieving
    map.put(key1, "value1");
    assertEquals("value1", map.get(key1));

    // Test same s3URI and etag should be treated as same key
    map.put(key2, "value2");
    assertEquals("value2", map.get(key1));
    assertEquals(1, map.size());

    // Test different etag should be treated as different key
    map.put(key3, "value3");
    assertEquals("value3", map.get(key3));
    assertEquals(2, map.size());

    // Test different s3URI should be treated as different key
    map.put(key4, "value4");
    assertEquals("value4", map.get(key4));
    assertEquals(3, map.size());
  }

  @Test
  void testEqualsAndHashCodeObjectKey() {
    S3URI s3uri1 = S3URI.of("bucket", "key");
    S3URI s3uri2 = S3URI.of("bucket", "key");
    S3URI s3uri3 = S3URI.of("bucket", "key2");

    ObjectKey key1 = ObjectKey.builder().s3URI(s3uri1).etag("etag1").build();

    ObjectKey key2 = ObjectKey.builder().s3URI(s3uri2).etag("etag1").build();

    ObjectKey key3 = ObjectKey.builder().s3URI(s3uri1).etag("etag2").build();

    ObjectKey key4 = ObjectKey.builder().s3URI(s3uri3).etag("etag1").build();

    // Test equals
    assertEquals(key1, key1); // Same object
    assertEquals(key1, key2); // Same values
    assertNotEquals(key1, key3); // Different etag
    assertNotEquals(key1, key4); // Different s3URI
    assertNotEquals(key1, null); // Null comparison
    assertNotEquals(key1, "not an ObjectKey"); // Different type

    // Test hashCode
    assertEquals(key1.hashCode(), key2.hashCode()); // Same values should have same hash
    assertNotEquals(key1.hashCode(), key3.hashCode()); // Different etag should have different hash
    assertNotEquals(key1.hashCode(), key4.hashCode()); // Different s3URI should have different hash
  }

  @Test
  void testNullValidation() {
    S3URI s3uri = S3URI.of("bucket", "key");

    // Test null s3URI
    assertThrows(
        NullPointerException.class,
        () -> {
          ObjectKey.builder().s3URI(null).etag("etag1").build();
        });

    // Test null etag
    assertThrows(
        NullPointerException.class,
        () -> {
          ObjectKey.builder().s3URI(s3uri).etag(null).build();
        });

    // Test constructor with null values
    assertThrows(
        NullPointerException.class,
        () -> {
          new ObjectKey(null, "etag1");
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new ObjectKey(s3uri, null);
        });
  }
}
