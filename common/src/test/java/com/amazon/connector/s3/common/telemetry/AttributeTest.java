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
package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class AttributeTest {
  @Test
  void testCreateAttribute() {
    Attribute attribute = Attribute.of("Foo", 42);
    assertEquals(attribute.getName(), "Foo");
    assertEquals(attribute.getValue(), 42);
  }

  @Test
  void testCreateAttributeNullNameThrows() {
    assertThrows(NullPointerException.class, () -> Attribute.of(null, 42));
  }

  @Test
  void testCreateAttributeNullValueThrows() {
    assertThrows(NullPointerException.class, () -> Attribute.of("Foo", null));
  }

  @Test
  void testCreateAttributeNullNameAndValueThrows() {
    assertThrows(NullPointerException.class, () -> Attribute.of(null, null));
  }

  @Test
  void testEquality() {
    assertEquals(Attribute.of("Foo", 42), Attribute.of("Foo", 42));
    assertNotEquals(Attribute.of("Foo", 42), Attribute.of("Foo", 43));
    assertNotEquals(Attribute.of("Bar", 42), Attribute.of("Foo", 42));
    assertNotEquals(Attribute.of("Foo", 42), null);
  }

  @Test
  void testHashCode() {
    assertEquals(Attribute.of("Foo", 42).hashCode(), Attribute.of("Foo", 42).hashCode());
    assertNotEquals(Attribute.of("Foo", 42).hashCode(), Attribute.of("Foo", 43).hashCode());
    assertNotEquals(Attribute.of("Bar", 42).hashCode(), Attribute.of("Foo", 42).hashCode());
  }

  @Test
  void testToString() {
    assertEquals("Attribute(Foo, 42)", Attribute.of("Foo", 42).toString());
  }
}
