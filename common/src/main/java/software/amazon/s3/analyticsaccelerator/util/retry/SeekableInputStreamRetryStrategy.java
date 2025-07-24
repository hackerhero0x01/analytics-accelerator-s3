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
package software.amazon.s3.analyticsaccelerator.util.retry;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.FailsafeExecutor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;

/**
 * Retry strategy implementation for seekable input stream operations. Uses Failsafe library to
 * execute operations with configurable retry policies.

 * This strategy will be additive to readTimeout and readRetryCount set on PhysicalIO configuration.
 */
public class SeekableInputStreamRetryStrategy implements RetryStrategy {
  private final List<RetryPolicy> retryPolicies;
  FailsafeExecutor<Object> failsafeExecutor;

  /** Creates a retry strategy with no retry policies (no retries). */
  public SeekableInputStreamRetryStrategy() {
    this.retryPolicies = new ArrayList<>();
    this.failsafeExecutor = Failsafe.none();
  }

  /**
   * Creates a retry strategy with one or more retry policies.
   *
   * @param outerPolicy the primary retry policy (required)
   * @param policies additional retry policies (optional)
   */
  @SuppressWarnings("varargs")
  public SeekableInputStreamRetryStrategy(RetryPolicy outerPolicy, RetryPolicy... policies) {
    Preconditions.checkNotNull(outerPolicy);
    this.retryPolicies = new ArrayList<>();
    this.retryPolicies.add(outerPolicy);
    if (policies != null && policies.length > 0) {
      this.retryPolicies.addAll(Arrays.asList(policies));
    }
    this.failsafeExecutor = Failsafe.with(getDelegates());
  }

  /**
   * Creates a retry strategy with a list of retry policies.
   *
   * @param policies the list of retry policies to apply
   */
  public SeekableInputStreamRetryStrategy(List<RetryPolicy> policies) {
    Preconditions.checkNotNull(policies);
    this.retryPolicies = new ArrayList<>();
    this.retryPolicies.addAll(policies);
    this.failsafeExecutor = Failsafe.with(getDelegates());
  }

  /**
   * Executes a runnable operation with retry logic.
   *
   * @param runnable the operation to execute
   * @throws IOException if the operation fails after all retries
   */
  @Override
  public void execute(IORunnable runnable) throws IOException {
    try {
      this.failsafeExecutor.run(runnable::apply);
    } catch (Exception ex) {
      throw handleExceptionAfterRetry(ex);
    }
  }

  /**
   * Executes a supplier operation with retry logic.
   *
   * @param <T> return type of the supplier
   * @param supplier the operation that returns a byte array
   * @return the result of the supplier operation
   * @throws IOException if the operation fails after all retries
   */
  @Override
  public <T> T get(IOSupplier<T> supplier) throws IOException {
    try {
      return this.failsafeExecutor.get(supplier::apply);
    } catch (Exception ex) {
      throw handleExceptionAfterRetry(ex);
    }
  }

  @Override
  public RetryStrategy amend(RetryPolicy policy) {
    Preconditions.checkNotNull(policy);
    this.failsafeExecutor = this.failsafeExecutor.compose(policy.getDelegate());
    return this;
  }

  @Override
  public RetryStrategy merge(RetryStrategy strategy) {
    Preconditions.checkNotNull(strategy);
    for (RetryPolicy policy : strategy.getRetryPolicies()) {
      this.failsafeExecutor = this.failsafeExecutor.compose(policy.getDelegate());
    }
    return this;
  }

  @Override
  public List<RetryPolicy> getRetryPolicies() {
    return this.retryPolicies;
  }

  /**
   * Converts retry policies to their Failsafe delegate policies.
   *
   * @return list of Failsafe policies
   */
  private List<dev.failsafe.Policy<Object>> getDelegates() {
    return this.retryPolicies.stream().map(RetryPolicy::getDelegate).collect(Collectors.toList());
  }

  /**
   * Handles exceptions after retry attempts are exhausted.
   *
   * @param e the exception that occurred
   * @return an IOException to throw
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
