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
package com.amazon.connector.s3.arbitraries;

import static com.amazon.connector.s3.util.Constants.ONE_MB;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Provide;

public class StreamArbitraries {

  private static final int MAX_STREAM_LENGTH_UNDER_TEST = 16 * ONE_MB;

  @Provide
  Arbitrary<Integer> streamSizes() {
    return Arbitraries.integers().between(0, MAX_STREAM_LENGTH_UNDER_TEST);
  }

  @Provide
  Arbitrary<Integer> positiveStreamSizes() {
    return Arbitraries.integers().between(1, MAX_STREAM_LENGTH_UNDER_TEST);
  }

  @Provide
  Arbitrary<Integer> sizeBiggerThanOne() {
    return Arbitraries.integers().between(2, MAX_STREAM_LENGTH_UNDER_TEST);
  }

  @Provide
  Arbitrary<Integer> validPositions() {
    return Arbitraries.integers().between(0, MAX_STREAM_LENGTH_UNDER_TEST);
  }

  @Provide
  Arbitrary<Integer> invalidPositions() {
    return Arbitraries.integers().lessOrEqual(-1);
  }

  @Provide
  Arbitrary<Integer> bufferSizes() {
    return Arbitraries.integers().between(1, 16 * 1024 * 1024);
  }
}
