package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class OperationContextTest {
  @Test
  void testPushAndPop() {
    OperationContext context = new OperationContext();
    Operation operation =
        Operation.builder()
            .name("read")
            .attribute("s3.bucket", "bucket")
            .attribute("s3.key", "key")
            .context(context)
            .build();
    context.pushOperation(operation);
    try {
      assertEquals(operation, context.getCurrentOperation());
      assertTrue(context.getCurrentNonDefaultOperation().isPresent());
      assertEquals(operation, context.getCurrentNonDefaultOperation().get());
    } finally {
      context.popOperation(operation);
      assertFalse(context.getCurrentNonDefaultOperation().isPresent());
    }
  }

  @Test
  void testPushAndPopDefault() {
    Operation operation =
        Operation.builder()
            .name("read")
            .attribute("s3.bucket", "bucket")
            .attribute("s3.key", "key")
            .build();
    OperationContext.DEFAULT.pushOperation(operation);
    try {
      assertEquals(operation, OperationContext.DEFAULT.getCurrentOperation());
      assertTrue(OperationContext.DEFAULT.getCurrentNonDefaultOperation().isPresent());
      assertEquals(operation, OperationContext.DEFAULT.getCurrentNonDefaultOperation().get());
    } finally {
      OperationContext.DEFAULT.popOperation(operation);
      assertFalse(OperationContext.DEFAULT.getCurrentNonDefaultOperation().isPresent());
    }
  }

  @Test
  void testNulls() {
    OperationContext context = new OperationContext();
    assertThrows(NullPointerException.class, () -> context.popOperation(null));
    assertThrows(NullPointerException.class, () -> context.pushOperation(null));
  }

  @Test
  void testPopEmptyStack() {
    OperationContext context = new OperationContext();
    Operation operation =
        Operation.builder()
            .name("read")
            .attribute("s3.bucket", "bucket")
            .attribute("s3.key", "key")
            .context(context)
            .build();
    assertThrows(IllegalStateException.class, () -> context.popOperation(operation));
  }

  @Test
  void testPopMismatchedStack() {
    OperationContext context = new OperationContext();
    Operation operation_on_stack =
        Operation.builder()
            .name("read1")
            .attribute("s3.bucket", "bucket1")
            .attribute("s3.key", "key1")
            .context(context)
            .build();

    Operation operation =
        Operation.builder()
            .name("read2")
            .attribute("s3.bucket", "bucket2")
            .attribute("s3.key", "key2")
            .context(context)
            .build();
    context.pushOperation(operation_on_stack);
    try {
      assertThrows(IllegalStateException.class, () -> context.popOperation(operation));
    } finally {
      context.popOperation(operation_on_stack);
    }
  }
}
