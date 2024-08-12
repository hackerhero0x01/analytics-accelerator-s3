package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class OperationTest {
  @Test
  void testCreateOperation() {
    Operation operation = Operation.builder().name("S3.GET").build();
    assertEquals("S3.GET", operation.getName());
    assertNotNull(operation.getId());
    assertSame(OperationContext.DEFAULT, operation.getContext());

    assertEquals(1, operation.getAttributes().size());
    assertEquals(
        Thread.currentThread().getId(),
        operation.getAttributes().get(CommonAttributes.THREAD_ID.getName()).getValue());

    // Assert immutability
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          operation.getAttributes().put("Foo", Attribute.of("Foo", "Bar"));
        });
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          operation.getAttributes().clear();
        });
  }

  @Test
  void testCreateOperationConstructor1() {
    Map<String, Attribute> attributes = new HashMap<>();
    attributes.put("Foo", Attribute.of("Foo", 42));
    Operation parent = Operation.builder().name("S3.GET").build();
    OperationContext context = new OperationContext();
    Operation operation = new Operation("id", "name", attributes, context, Optional.of(parent));

    assertEquals("id", operation.getId());
    assertEquals("name", operation.getName());
    assertEquals(
        Thread.currentThread().getId(),
        operation.getAttributes().get(CommonAttributes.THREAD_ID.getName()).getValue());
    assertSame(context, operation.getContext());
    assertTrue(operation.getParent().isPresent());
    assertSame(parent, operation.getParent().get());
  }

  @Test
  void testCreateOperationConstructor1WithNulls() {
    Map<String, Attribute> attributes = new HashMap<>();
    attributes.put("Foo", Attribute.of("Foo", 42));
    Operation parent = Operation.builder().name("S3.GET").build();
    OperationContext context = new OperationContext();

    assertThrows(
        NullPointerException.class,
        () -> new Operation(null, "name", attributes, context, Optional.of(parent)));
    assertThrows(
        NullPointerException.class,
        () -> new Operation("id", null, attributes, context, Optional.of(parent)));
    assertThrows(
        NullPointerException.class,
        () -> new Operation("id", "name", null, context, Optional.of(parent)));
    assertThrows(
        NullPointerException.class,
        () -> new Operation("id", "name", attributes, null, Optional.of(parent)));
    assertThrows(
        NullPointerException.class, () -> new Operation("id", "name", attributes, context, null));
  }

  @Test
  void testCreateOperationConstructor2() {
    Map<String, Attribute> attributes = new HashMap<>();
    attributes.put("Foo", Attribute.of("Foo", 42));
    Operation parent = Operation.builder().name("S3.GET").build();
    OperationContext context = new OperationContext();
    Operation operation =
        new Operation("id", "name", attributes, context, Optional.of(parent), true);

    assertEquals("id", operation.getId());
    assertEquals("name", operation.getName());
    assertEquals(
        Thread.currentThread().getId(),
        operation.getAttributes().get(CommonAttributes.THREAD_ID.getName()).getValue());
    assertSame(context, operation.getContext());
    assertTrue(operation.getParent().isPresent());
    assertSame(parent, operation.getParent().get());
  }

  @Test
  void testCreateOperationConstructor2WithNulls() {
    Map<String, Attribute> attributes = new HashMap<>();
    attributes.put("Foo", Attribute.of("Foo", 42));
    Operation parent = Operation.builder().name("S3.GET").build();
    OperationContext context = new OperationContext();

    assertThrows(
        NullPointerException.class,
        () -> new Operation(null, "name", attributes, context, Optional.of(parent), true));
    assertThrows(
        NullPointerException.class,
        () -> new Operation("id", null, attributes, context, Optional.of(parent), true));
    assertThrows(
        NullPointerException.class,
        () -> new Operation("id", "name", null, context, Optional.of(parent), true));
    assertThrows(
        NullPointerException.class,
        () -> new Operation("id", "name", attributes, null, Optional.of(parent), true));
    assertThrows(
        NullPointerException.class,
        () -> new Operation("id", "name", attributes, context, null, true));
  }

  @Test
  void testCreateOperationWithId() {
    Operation operation = Operation.builder().name("S3.GET").id("Blah").build();
    assertEquals("S3.GET", operation.getName());
    assertEquals("Blah", operation.getId());
    assertSame(OperationContext.DEFAULT, operation.getContext());

    assertEquals(1, operation.getAttributes().size());
    assertEquals(
        Thread.currentThread().getId(),
        operation.getAttributes().get(CommonAttributes.THREAD_ID.getName()).getValue());

    // Assert immutability
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          operation.getAttributes().put("Foo", Attribute.of("Foo", "Bar"));
        });
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          operation.getAttributes().clear();
        });
  }

  @Test
  void testCreateOperationWithAttributesNameAndValue() {
    Operation operation =
        Operation.builder()
            .name("S3.GET")
            .attribute("s3.bucket", "bucket")
            .attribute("s3.key", "key")
            .build();
    assertNotNull(operation.getId());
    assertEquals("S3.GET", operation.getName());
    assertSame(OperationContext.DEFAULT, operation.getContext());

    assertEquals(3, operation.getAttributes().size());
    assertEquals("bucket", operation.getAttributes().get("s3.bucket").getValue());
    assertEquals("key", operation.getAttributes().get("s3.key").getValue());
    assertEquals(
        Thread.currentThread().getId(),
        operation.getAttributes().get(CommonAttributes.THREAD_ID.getName()).getValue());
    assertFalse(operation.getParent().isPresent());

    // Assert immutability
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          operation.getAttributes().put("Foo", Attribute.of("Foo", "Bar"));
        });
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          operation.getAttributes().clear();
        });
  }

  @Test
  void testCreateOperationWithAttributes() {
    Operation operation =
        Operation.builder()
            .name("S3.GET")
            .attribute(Attribute.of("s3.bucket", "bucket"))
            .attribute(Attribute.of("s3.key", "key"))
            .build();
    assertNotNull(operation.getId());
    assertEquals("S3.GET", operation.getName());
    assertSame(OperationContext.DEFAULT, operation.getContext());

    assertEquals(3, operation.getAttributes().size());
    assertEquals("bucket", operation.getAttributes().get("s3.bucket").getValue());
    assertEquals("key", operation.getAttributes().get("s3.key").getValue());
    assertEquals(
        Thread.currentThread().getId(),
        operation.getAttributes().get(CommonAttributes.THREAD_ID.getName()).getValue());
    assertFalse(operation.getParent().isPresent());

    // Assert immutability
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          operation.getAttributes().put("Foo", Attribute.of("Foo", "Bar"));
        });
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          operation.getAttributes().clear();
        });
  }

  @Test
  void testCreateOperationWithAttributesWithParent() {
    Operation parent =
        Operation.builder()
            .name("read")
            .attribute("s3.bucket", "bucket")
            .attribute("s3.key", "key")
            .build();
    Operation operation =
        Operation.builder()
            .name("S3.GET")
            .attribute("s3.bucket", "bucket")
            .attribute("s3.key", "key")
            .parent(parent)
            .build();
    assertEquals("S3.GET", operation.getName());
    assertEquals(3, operation.getAttributes().size());
    assertEquals("bucket", operation.getAttributes().get("s3.bucket").getValue());
    assertEquals("key", operation.getAttributes().get("s3.key").getValue());
    assertEquals(
        Thread.currentThread().getId(),
        operation.getAttributes().get(CommonAttributes.THREAD_ID.getName()).getValue());
    assertTrue(operation.getParent().isPresent());
    assertEquals(parent, operation.getParent().get());
    assertSame(OperationContext.DEFAULT, operation.getContext());

    // Assert immutability
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          operation.getAttributes().put("Foo", Attribute.of("Foo", "Bar"));
        });
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          operation.getAttributes().clear();
        });
  }

  @Test
  void testCreateOperationWithAttributesWithImplicitParent() {
    Operation parent =
        Operation.builder()
            .name("read")
            .attribute("s3.bucket", "bucket")
            .attribute("s3.key", "key")
            .build();

    OperationContext operationContext = new OperationContext();
    operationContext.pushOperation(parent);
    try {
      Operation operation =
          Operation.builder()
              .name("S3.GET")
              .attribute("s3.bucket", "bucket")
              .attribute("s3.key", "key")
              .context(operationContext)
              .build();
      assertEquals("S3.GET", operation.getName());
      assertEquals(3, operation.getAttributes().size());
      assertEquals("bucket", operation.getAttributes().get("s3.bucket").getValue());
      assertEquals("key", operation.getAttributes().get("s3.key").getValue());
      assertEquals(
          Thread.currentThread().getId(),
          operation.getAttributes().get(CommonAttributes.THREAD_ID.getName()).getValue());
      assertTrue(operation.getParent().isPresent());
      assertEquals(parent, operation.getParent().get());
      assertSame(operationContext, operation.getContext());

      // Assert immutability
      assertThrows(
          UnsupportedOperationException.class,
          () -> {
            operation.getAttributes().put("Foo", Attribute.of("Foo", "Bar"));
          });
      assertThrows(
          UnsupportedOperationException.class,
          () -> {
            operation.getAttributes().clear();
          });
    } finally {
      operationContext.popOperation(parent);
    }
  }

  @Test
  void testCreateOperationWithAttributesWithImplicitParentDefaultContext() {
    Operation parent =
        Operation.builder()
            .name("read")
            .attribute("s3.bucket", "bucket")
            .attribute("s3.key", "key")
            .build();

    OperationContext.DEFAULT.pushOperation(parent);
    try {
      Operation operation =
          Operation.builder()
              .name("S3.GET")
              .attribute("s3.bucket", "bucket")
              .attribute("s3.key", "key")
              .build();
      assertEquals("S3.GET", operation.getName());
      assertEquals(3, operation.getAttributes().size());
      assertEquals("bucket", operation.getAttributes().get("s3.bucket").getValue());
      assertEquals("key", operation.getAttributes().get("s3.key").getValue());
      assertEquals(
          Thread.currentThread().getId(),
          operation.getAttributes().get(CommonAttributes.THREAD_ID.getName()).getValue());
      assertTrue(operation.getParent().isPresent());
      assertEquals(parent, operation.getParent().get());
      assertSame(OperationContext.DEFAULT, operation.getContext());

      // Assert immutability
      assertThrows(
          UnsupportedOperationException.class,
          () -> {
            operation.getAttributes().put("Foo", Attribute.of("Foo", "Bar"));
          });
      assertThrows(
          UnsupportedOperationException.class,
          () -> {
            operation.getAttributes().clear();
          });
    } finally {
      OperationContext.DEFAULT.popOperation(parent);
    }
  }

  @Test
  void testEqualsAndHashcode() {
    Operation operation1 =
        Operation.builder().id("id1").name("S3.GET").attribute("foo", "bar").build();
    Operation operation2 =
        Operation.builder().id("id1").name("S3.GET").attribute("foo", "bar").build();
    Operation operation3 =
        Operation.builder().id("id2").name("S3.PUT").attribute("foo1", "bar1").build();
    assertEquals(operation1, operation2);
    assertEquals(operation1.hashCode(), operation2.hashCode());

    assertNotEquals(operation3, operation2);
    assertNotEquals(operation3.hashCode(), operation2.hashCode());
  }

  @Test
  void testToString() {
    Operation operation1 = Operation.builder().id("op1").name("S3.GET").build();
    Operation operation2 =
        Operation.builder()
            .id("op2")
            .name("S3.GET")
            .attribute("foo", "bar")
            .attribute("x", "y")
            .build();
    Operation operation3 = Operation.builder().id("op3").name("S3.GET").parent(operation1).build();
    Operation operation4 =
        Operation.builder()
            .id("op4")
            .name("S3.GET")
            .attribute("foo", "bar")
            .parent(operation1)
            .build();
    String threadAttributeAsString =
        CommonAttributes.THREAD_ID.getName() + "=" + Thread.currentThread().getId();

    assertEquals("[op1] S3.GET(" + threadAttributeAsString + ")", operation1.toString());
    assertEquals(
        "[op2] S3.GET(" + threadAttributeAsString + ", foo=bar, x=y)", operation2.toString());
    assertEquals("[op3<-op1] S3.GET(" + threadAttributeAsString + ")", operation3.toString());
    assertEquals(
        "[op4<-op1] S3.GET(" + threadAttributeAsString + ", foo=bar)", operation4.toString());
  }

  @Test
  void testCreateOperationWithNullsMustThrow() {
    assertThrows(
        NullPointerException.class,
        () -> {
          Operation.builder().name(null).build();
        });
    assertThrows(
        NullPointerException.class,
        () -> {
          Operation.builder().build();
        });
    assertThrows(
        NullPointerException.class,
        () -> {
          Operation.builder().id(null).name("foo").build();
        });
    assertThrows(
        NullPointerException.class,
        () -> {
          Operation.builder().name("foo").attribute(null, "bar").build();
        });
    assertThrows(
        NullPointerException.class,
        () -> {
          Operation.builder().name("foo").attribute("foo", null).build();
        });
    assertThrows(
        NullPointerException.class,
        () -> {
          Operation.builder().name("foo").attribute(null).build();
        });
    assertThrows(
        NullPointerException.class,
        () -> {
          Operation.builder().name("foo").parent(null).build();
        });
    assertThrows(
        NullPointerException.class,
        () -> {
          Operation.builder().name("foo").context(null).build();
        });
  }

  @Test
  void testCreateOperationWithDuplicateAttributeNameMustThrow() {
    Operation.OperationBuilder builder = Operation.builder().name("foo").attribute("foo", "bar");
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          builder.attribute("foo", "bar").build();
        });
  }
}
