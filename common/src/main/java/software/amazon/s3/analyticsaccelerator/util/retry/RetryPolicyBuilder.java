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

import java.time.Duration;
import java.util.List;

/**
 * Builder for creating RetryPolicy instances with custom configurations. Wraps the Failsafe
 * RetryPolicyBuilder to provide a simplified interface.
 */
public class RetryPolicyBuilder {
  private final dev.failsafe.RetryPolicyBuilder<byte[]> delegateBuilder;

  /** Creates a new RetryPolicyBuilder instance. */
  protected RetryPolicyBuilder() {
    this.delegateBuilder = dev.failsafe.RetryPolicy.builder();
  }

  /**
   * Sets the maximum number of retry attempts.
   *
   * @param maxRetries the maximum number of retries
   * @return this builder instance
   */
  public RetryPolicyBuilder withMaxRetries(int maxRetries) {
    delegateBuilder.withMaxRetries(maxRetries);
    return this;
  }

  /**
   * Sets the delay between retry attempts.
   *
   * @param delay the delay duration between retries
   * @return this builder instance
   */
  public RetryPolicyBuilder withDelay(Duration delay) {
    delegateBuilder.withDelay(delay);
    return this;
  }

  /**
   * Specifies an exception type to handle for retries.
   *
   * @param exception the exception class to handle
   * @return this builder instance
   */
  public RetryPolicyBuilder handle(Class<? extends Throwable> exception) {
    delegateBuilder.handle(exception);
    return this;
  }

  /**
   * Specifies multiple exception types to handle for retries.
   *
   * @param exceptions the exception classes to handle
   * @return this builder instance
   */
  @SafeVarargs
  @SuppressWarnings("varargs")
  public final RetryPolicyBuilder handle(Class<? extends Throwable>... exceptions) {
    delegateBuilder.handle(exceptions);
    return this;
  }

  /**
   * Specifies a list of exception types to handle for retries.
   *
   * @param exceptions the list of exception classes to handle
   * @return this builder instance
   */
  @SuppressWarnings("varargs")
  public final RetryPolicyBuilder handle(List<Class<? extends Throwable>> exceptions) {
    delegateBuilder.handle(exceptions);
    return this;
  }

  /**
   * Sets a callback to execute on each retry attempt.
   *
   * @param onRetry the callback to execute on retry
   * @return this builder instance
   */
  public RetryPolicyBuilder onRetry(Runnable onRetry) {
    delegateBuilder.onRetry(event -> onRetry.run());
    return this;
  }

  /**
   * Builds the RetryPolicy with the configured settings.
   *
   * @return a new RetryPolicy instance
   */
  public RetryPolicy build() {
    dev.failsafe.RetryPolicy<byte[]> delegate = delegateBuilder.build();

    return new RetryPolicyImpl(delegate);
  }

  private static class RetryPolicyImpl implements RetryPolicy {
    private final dev.failsafe.RetryPolicy<byte[]> delegate;

    RetryPolicyImpl(dev.failsafe.RetryPolicy<byte[]> delegate) {
      this.delegate = delegate;
    }

    @Override
    public dev.failsafe.RetryPolicy<byte[]> getDelegate() {
      return delegate;
    }
  }
}
