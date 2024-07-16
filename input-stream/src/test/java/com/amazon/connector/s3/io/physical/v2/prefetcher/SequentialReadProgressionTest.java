package com.amazon.connector.s3.io.physical.v2.prefetcher;

import static com.amazon.connector.s3.util.Constants.ONE_MB;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SequentialReadProgressionTest {

  @Test
  public void test__sequentialReadProgression__asExpected() {
    // Given: a SequentialReadProgression
    SequentialReadProgression sequentialReadProgression = new SequentialReadProgression();

    // When & Then: size is requested for a generation --> size is correct
    assertEquals(2 * ONE_MB, sequentialReadProgression.getSizeForGeneration(0));
    assertEquals(8 * ONE_MB, sequentialReadProgression.getSizeForGeneration(1));
    assertEquals(32 * ONE_MB, sequentialReadProgression.getSizeForGeneration(2));
    assertEquals(128 * ONE_MB, sequentialReadProgression.getSizeForGeneration(3));
  }
}
