package com.amazon.connector.s3.common.telemetry;

import com.amazon.connector.s3.common.Preconditions;
import java.util.HashMap;
import java.util.Optional;
import java.util.Stack;
import lombok.NonNull;

/** Context for carried telemetry contexts */
public final class OperationContext {
  // Important: we intentionally do not try to infer the parent in this case, as the default
  // operation can never have one.
  /**
   * "Default" operation - this is what is in progress where no other operations are in progress.
   */
  private static final String DEFAULT_OPERATION_NAME = "default";

  private final Operation defaultOperation =
      new Operation(
          DEFAULT_OPERATION_NAME,
          DEFAULT_OPERATION_NAME,
          new HashMap<String, Attribute>(),
          this,
          Optional.empty(),
          false);
  /**
   * Operation stack. This is to track nested operations and establish implicit parenting. Every
   * time a new operation is "started", it is pushed on the stack. After the execution is completed,
   * the operation is "popped". This helps with implicit parenting - if the parent is not
   * specifically set, the stack is consulted, and the latest operation on the stack is used as a
   * parent. This allows for implicit parenting of operations.
   */
  private final ThreadLocal<Stack<Operation>> operationsStack =
      ThreadLocal.withInitial(
          () -> {
            Stack<Operation> stack = new Stack<>();
            stack.push(this.defaultOperation);
            return stack;
          });

  /** Creates a new instance of {@link OperationContext}. */
  public OperationContext() {}

  /**
   * Default operation context. This will probably be used in most cases, and allows for multiple
   * instances of {@link Telemetry} to share the same {@link OperationContext}, thus correctly
   * chaining operations together. In scenarios where a different context should be used for a group
   * of {@link Operation}s, a new instance of {@link OperationContext} can be created and passed to
   * the {@link Operation#getContext()}.
   */
  public static OperationContext DEFAULT = new OperationContext();
  /**
   * Returns the top operation on the stack.
   *
   * @return top operation on the stack.
   */
  public Operation getCurrentOperation() {
    Stack<Operation> operationsStack = this.operationsStack.get();
    // Stack should never be empty, as the default operation is always in progress
    Preconditions.checkState(!operationsStack.isEmpty());
    return operationsStack.peek();
  }

  /**
   * Returns the top operation on the stack.
   *
   * @return top operation on the stack.
   */
  public Optional<Operation> getCurrentNonDefaultOperation() {
    Operation currentOperation = this.getCurrentOperation();
    return (currentOperation.equals(defaultOperation))
        ? Optional.empty()
        : Optional.of(currentOperation);
  }

  /**
   * Pushes the {@link Operation} on the stack of the current thread. This should be invoked
   * **before** we start executing code that can create new operations.
   *
   * @param operation the {@link Operation} to push on the stack.
   */
  public void pushOperation(@NonNull Operation operation) {
    this.operationsStack.get().push(operation);
  }

  /**
   * This pops the {@link Operation} off the current thread stack, presuming it is the latest one.
   * if it isn't, {@link IllegalStateException} is thrown.
   *
   * @param operation the {@link Operation} to pop pff the stack.
   */
  public void popOperation(@NonNull Operation operation) {
    Operation currentOperation = this.getCurrentOperation();

    if (!currentOperation.equals(operation)) {
      throw new IllegalStateException(
          String.format(
              "The operation `%s` being popped is not equal to the current operation `%s`",
              operation, currentOperation));
    }

    this.operationsStack.get().pop();
  }
}
