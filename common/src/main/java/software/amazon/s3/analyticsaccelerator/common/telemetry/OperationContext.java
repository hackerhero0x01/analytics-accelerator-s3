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
package software.amazon.s3.analyticsaccelerator.common.telemetry;

import java.util.EmptyStackException;
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

  final Operation defaultOperation =
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
  public static final OperationContext DEFAULT = new OperationContext();
  /**
   * Returns the top operation on the stack.
   *
   * @return top operation on the stack.
   */
  public Operation getCurrentOperation() {
    Stack<Operation> operationsStack = this.operationsStack.get();
    try {
      return operationsStack.peek();
    } catch (EmptyStackException exception) {
      throw new IllegalStateException("The operation stack must not be empty", exception);
    }
  }

  /**
   * Returns the top operation on the stack.
   *
   * @return top operation on the stack.
   */
  public Optional<Operation> getCurrentNonDefaultOperation() {
    Operation currentOperation = this.getCurrentOperation();
    return (currentOperation == defaultOperation)
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
    // There is some logic here that is the same as is `getCurrentOperation`.
    // We inline it here so that we ony get the stack from the TLC onc, to reduce overhead.
    Stack<Operation> operationsStack = this.operationsStack.get();
    try {
      Operation currentOperation = operationsStack.peek();
      if (currentOperation != operation) {
        throw new IllegalStateException(
            String.format(
                "The operation `%s` being popped is not equal to the current operation `%s`",
                operation, currentOperation));
      }

    } catch (EmptyStackException exception) {
      throw new IllegalStateException("The operation stack must not be empty", exception);
    }
    operationsStack.pop();
  }
}
