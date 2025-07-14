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
package software.amazon.s3.analyticsaccelerator.retry;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.Policy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;

/**
 * Implementation of Strategy that uses Failsafe library. A strategy is a collection of policies.
 *
 * <p>This class encapsulates all Failsafe-specific code to hide the dependency from AAL users. It
 * supports multiple retry policies that are applied in sequence, allowing for complex retry
 * strategies such as combining different policies for different types of failures.
 *
 * <p>The executor maintains a list of retry policies and creates a Failsafe executor that applies
 * these policies when executing operations that may fail.
 *
 * @param <R> the result type of operations executed by this retry executor
 */
public class SeekableInputStreamRetryStrategy<R> implements RetryStrategy<R> {
  private final List<RetryPolicy<R>> retryPolicies;
  FailsafeExecutor<R> failsafeExecutor;

  /**
   * Creates a no-op executor with no retry policies.
   *
   * <p>Operations executed with this executor will not be retried on failure. This constructor is
   * primarily used for testing or scenarios where retry behavior is not desired.
   */
  public SeekableInputStreamRetryStrategy() {
    this.retryPolicies = new ArrayList<>();
    this.failsafeExecutor = Failsafe.none();
  }

  /**
   * Creates a retry executor with multiple retry policies.
   *
   * <p>The policies are applied in the order they are provided, with the outerPolicy being applied
   * first, followed by the additional policies. This allows for layered retry strategies where
   * different policies handle different aspects of failure recovery.
   *
   * @param outerPolicy the primary retry policy to apply
   * @param policies additional retry policies to apply after the outer policy
   * @throws NullPointerException if outerPolicy is null
   */
  @SafeVarargs
  @SuppressWarnings("varargs")
  public SeekableInputStreamRetryStrategy(RetryPolicy<R> outerPolicy, RetryPolicy<R>... policies) {
    Preconditions.checkNotNull(outerPolicy);
    this.retryPolicies = new ArrayList<>();
    this.retryPolicies.add(outerPolicy);
    if (policies != null && policies.length > 0) {
      this.retryPolicies.addAll(Arrays.asList(policies));
    }
    this.failsafeExecutor = Failsafe.with(getDelegates());
  }

  /**
   * Executes a runnable with retry logic using Failsafe internally.
   *
   * @param runnable The operation to execute
   * @throws IOException if the operation fails after all retries
   */
  @Override
  public void execute(IORunnable runnable) throws IOException {
    try {
      this.failsafeExecutor.run(runnable::run);
    } catch (Exception ex) {
      throw handleExceptionAfterRetry(ex);
    }
  }

  @Override
  public R get(IOSupplier<R> supplier) throws IOException {
    try {
      return this.failsafeExecutor.get(supplier::get);
    } catch (Exception ex) {
      throw handleExceptionAfterRetry(ex);
    }
  }

  /**
   * Extracts the underlying Failsafe retry policies from the wrapper retry policies.
   *
   * <p>This method is used internally to convert the list of RetryPolicy wrappers into the actual
   * Failsafe RetryPolicy instances that can be used with the Failsafe executor.
   *
   * @return a list of Failsafe RetryPolicy instances
   */
  private List<Policy<R>> getDelegates() {
    return this.retryPolicies.stream().map(RetryPolicy::getDelegate).collect(Collectors.toList());
  }

  /**
   * If functional interface throws a checked exception, failsafe will wrap it around a
   * FailsafeException. This method unwraps the cause and throws the original exception. If
   * functional interface throws an unchecked exception, this method will catch it and throw an
   * IOException instead.
   *
   * @param e Exception thrown by functional interface
   * @return IOException
   */
  private IOException handleExceptionAfterRetry(Exception e) {
    IOException toThrow = new IOException("Failed to execute operation with retries", e);

    if (e instanceof FailsafeException) {
      Optional<Throwable> cause = Optional.ofNullable(e.getCause());
      if (cause.isPresent()) {
        if (cause.get() instanceof IOException) {
          return (IOException) cause.get();
        } else {
          toThrow = new IOException("Failed to execute operation with retries", cause.get());
        }
      }
    }
    return toThrow;
  }
}
