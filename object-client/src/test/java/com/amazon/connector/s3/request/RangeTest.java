package com.amazon.connector.s3.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.OptionalLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class RangeTest {

  @Test
  void testConstructorThrowsOnNull() {
    assertThrows(NullPointerException.class, () -> new Range(null, OptionalLong.empty()));
    assertThrows(NullPointerException.class, () -> new Range(OptionalLong.empty(), null));
  }

  @ParameterizedTest
  @MethodSource("invalidRanges")
  void testInvalidRangesThrow(OptionalLong start, OptionalLong end) {
    assertThrows(IllegalArgumentException.class, () -> new Range(start, end));
  }

  @ParameterizedTest
  @MethodSource("validRanges")
  void testToString(OptionalLong start, OptionalLong end, String expected) {
    assertEquals(expected, new Range(start, end).toString());
  }

  static Stream<Arguments> validRanges() {
    return Stream.of(
        Arguments.of(OptionalLong.of(1), OptionalLong.of(5), "bytes=1-5"),
        Arguments.of(OptionalLong.empty(), OptionalLong.of(5), "bytes=-5"),
        Arguments.of(OptionalLong.of(1), OptionalLong.empty(), "bytes=1-"));
  }

  static Stream<Arguments> invalidRanges() {
    return Stream.of(
        Arguments.of(OptionalLong.empty(), OptionalLong.empty()),
        Arguments.of(OptionalLong.of(-100), OptionalLong.of(5)),
        Arguments.of(OptionalLong.of(1), OptionalLong.of(-100)),
        Arguments.of(OptionalLong.of(-1), OptionalLong.of(-1)));
  }
}
