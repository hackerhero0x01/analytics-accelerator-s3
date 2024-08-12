package com.amazon.connector.s3.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class RangeTest {

  @ParameterizedTest
  @MethodSource("invalidRanges")
  void testInvalidRangesThrow(long start, long end) {
    assertThrows(IllegalArgumentException.class, () -> new Range(start, end));
  }

  @ParameterizedTest
  @MethodSource("validStringRanges")
  void testToString(long start, long end, String expected) {
    assertEquals(expected, new Range(start, end).toString());
  }

  @ParameterizedTest
  @MethodSource("validHttpStringRanges")
  void testToHttpString(long start, long end, String expected) {
    assertEquals(expected, new Range(start, end).toHttpString());
  }

  @Test
  void testSize() {
    assertEquals(1, new Range(0, 0).getSize());
    assertEquals(100, new Range(0, 99).getSize());
  }

  static Stream<Arguments> validStringRanges() {
    return Stream.of(
        Arguments.of(1, 5, "[1-5]"),
        Arguments.of(0, 0, "[0-0]"),
        Arguments.of(100, Long.MAX_VALUE, "[100-" + Long.MAX_VALUE + "]"));
  }

  static Stream<Arguments> validHttpStringRanges() {
    return Stream.of(
        Arguments.of(1, 5, "bytes=1-5"),
        Arguments.of(0, 0, "bytes=0-0"),
        Arguments.of(100, Long.MAX_VALUE, "bytes=100-" + Long.MAX_VALUE));
  }

  static Stream<Arguments> invalidRanges() {
    return Stream.of(
        Arguments.of(7, 5), Arguments.of(-100, 5), Arguments.of(1, -100), Arguments.of(-1, 1));
  }
}
