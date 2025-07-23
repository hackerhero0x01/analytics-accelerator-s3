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

import java.io.IOException;
import java.util.List;

/** Interface for executing operations with retry logic. */
public interface RetryStrategy {
  /**
   * Executes a runnable with retry logic.
   *
   * @param runnable the operation to execute
   * @throws IOException if the operation fails after all retry attempts
   */
  void execute(IORunnable runnable) throws IOException;

  /**
   * Executes a supplier with retry logic.
   *
   * @param supplier the operation to execute
   * @return result of the supplier
   * @throws IOException if the operation fails after all retry attempts
   */
  byte[] get(IOSupplier supplier) throws IOException;

  /**
   * Adds a retry policy to the strategy. This will be policy first to execute as it is appended to
   * the policy list.
   *
   * @param policy
   * @return a new {@link RetryStrategy} with the policy appended
   */
  RetryStrategy amend(RetryPolicy policy);

  /**
   * Merge two retry strategies and return a new {@link RetryStrategy}. This new strategy will
   * execute policies from both strategies in order.
   *
   * @param strategy
   * @return a new {@link RetryStrategy}
   */
  RetryStrategy merge(RetryStrategy strategy);

  /**
   * Get retry policies associated with a {@link RetryStrategy}
   *
   * @return list of {@link RetryPolicy}
   */
  List<RetryPolicy> getRetryPolicies();
}
