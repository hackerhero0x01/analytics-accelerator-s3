package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
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
    Operation operationOnStack =
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
    context.pushOperation(operationOnStack);
    try {
      assertThrows(IllegalStateException.class, () -> context.popOperation(operation));
    } finally {
      context.popOperation(operationOnStack);
    }
  }

  @Test
  void testEmptyStack() {
    OperationContext context = new OperationContext();
    Operation operation =
        Operation.builder()
            .name("read")
            .attribute("s3.bucket", "bucket")
            .attribute("s3.key", "key")
            .context(context)
            .build();
    // This should never be possible in practice, but this does clear teh stack
    context.popOperation(context.defaultOperation);

    assertThrows(IllegalStateException.class, () -> context.popOperation(operation));
    assertThrows(IllegalStateException.class, context::getCurrentOperation);
  }
}
