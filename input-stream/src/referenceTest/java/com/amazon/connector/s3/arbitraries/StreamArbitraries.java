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
