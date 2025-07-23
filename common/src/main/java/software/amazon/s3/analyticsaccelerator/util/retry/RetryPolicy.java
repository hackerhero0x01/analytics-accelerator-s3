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

/**
 * A retry policy interface that wraps the Failsafe retry policy for byte array operations. Provides
 * factory methods to create retry policies with default or custom configurations.
 */
public interface RetryPolicy {

  /**
   * Creates a new retry policy builder.
   *
   * @return a new RetryPolicyBuilder instance
   */
  static RetryPolicyBuilder builder() {
    return new RetryPolicyBuilder();
  }

  /**
   * Creates a retry policy with default settings.
   *
   * @return a RetryPolicy with default configuration
   */
  static RetryPolicy ofDefaults() {
    return RetryPolicy.builder().build();
  }

  /**
   * Gets the underlying Failsafe retry policy delegate.
   *
   * @return the Failsafe RetryPolicy for byte arrays
   */
  dev.failsafe.RetryPolicy<byte[]> getDelegate();
}
