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
import dev.failsafe.Timeout;
import dev.failsafe.TimeoutExceededException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;

/**
 * Retry strategy implementation for seekable input stream operations. Uses Failsafe library to
 * execute operations with configurable retry policies.
 *
 * <p>If provided with a timeout this strategy will overwrite readTimeout and readRetryCount set on
 * PhysicalIOConfiguration. If not, values from PhysicalIOConfiguration will be used to manage
 * storage read timeouts.
 */
public class DefaultRetryStrategyImpl implements RetryStrategy {
  private final List<RetryPolicy> retryPolicies;
  private Timeout<Object> timeoutPolicy;
  @Getter private boolean timeoutSet;

  /** Creates a retry strategy with no retry policies (no retries). */
  public DefaultRetryStrategyImpl() {
    this.retryPolicies = new ArrayList<>();
  }

  /**
   * Creates a retry strategy with one or more retry policies.
   *
   * @param outerPolicy the primary retry policy (required)
   * @param policies additional retry policies (optional)
   */
  @SuppressWarnings("varargs")
  public DefaultRetryStrategyImpl(RetryPolicy outerPolicy, RetryPolicy... policies) {
    Preconditions.checkNotNull(outerPolicy);
    this.retryPolicies = new ArrayList<>();
    this.retryPolicies.add(outerPolicy);
    if (policies != null && policies.length > 0) {
      this.retryPolicies.addAll(Arrays.asList(policies));
    }
  }

  /**
   * Creates a retry strategy with a list of retry policies.
   *
   * @param policies the list of retry policies to apply
   */
  public DefaultRetryStrategyImpl(List<RetryPolicy> policies) {
    Preconditions.checkNotNull(policies);
    this.retryPolicies = new ArrayList<>();
    this.retryPolicies.addAll(policies);
  }

  /**
   * Executes a runnable operation with retry logic.
   *
   * @param runnable the operation to execute
   */
  @Override
  @SneakyThrows
  public void execute(IORunnable runnable) {
    try {
      executor().run(runnable::apply);
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
   */
  @Override
  @SneakyThrows
  public <T> T get(IOSupplier<T> supplier) {
    try {
      return executor().get(supplier::apply);
    } catch (Exception ex) {
      throw handleExceptionAfterRetry(ex);
    }
  }

  @Override
  public RetryStrategy amend(RetryPolicy policy) {
    Preconditions.checkNotNull(policy);
    this.retryPolicies.add(policy);
    return this;
  }

  @Override
  public RetryStrategy merge(RetryStrategy strategy) {
    Preconditions.checkNotNull(strategy);
    this.retryPolicies.addAll(strategy.getRetryPolicies());
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
   * Handles exceptions after retry attempts are exhausted. This is needed to unwrap Failsafe
   * exception
   *
   * @param e the exception that occurred
   * @return an IOException to throw
   */
  private Exception handleExceptionAfterRetry(Exception e) {
    if (e instanceof FailsafeException) {
      Optional<Throwable> cause = Optional.ofNullable(e.getCause());
      if (cause.isPresent()) {
        return (Exception) cause.get();
      }
    }
    return e;
  }

  private FailsafeExecutor<Object> executor() {
    FailsafeExecutor<Object> executor;
    if (retryPolicies.isEmpty()) {
      executor = Failsafe.none();
    } else {
      executor = Failsafe.with(getDelegates());
    }
    if (this.timeoutSet) executor = executor.compose(timeoutPolicy);
    return executor;
  }

  /**
   * Create a timeout for read from storage operations and with specified retry count. This will
   * override settings in PhysicalIOConfiguration (blockreadtimeout and blockreadretrycount) if set.
   * If user does not set a timeout in their retry strategy, a timeout will be set based on
   * aforementioned configuration. set blockreadtimeout = 0 to disable timeouts
   *
   * @param timeoutDurationMillis Timeout duration for reading from storage
   * @param retryCount Number of times to retry if Timeout Exceeds
   */
  public void setTimeoutPolicy(long timeoutDurationMillis, int retryCount) {
    this.timeoutPolicy =
        Timeout.builder(Duration.ofMillis(timeoutDurationMillis)).withInterrupt().build();
    RetryPolicy timeoutRetries =
        RetryPolicy.builder()
            .handle(TimeoutExceededException.class)
            .withMaxRetries(retryCount)
            .build();
    this.retryPolicies.add(timeoutRetries);
    this.timeoutSet = true;
  }
}
