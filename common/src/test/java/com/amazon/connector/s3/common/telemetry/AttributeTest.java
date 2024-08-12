package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

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
    Attribute attribute = Attribute.of("Foo", 42);
    assertEquals(attribute, attribute);

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
