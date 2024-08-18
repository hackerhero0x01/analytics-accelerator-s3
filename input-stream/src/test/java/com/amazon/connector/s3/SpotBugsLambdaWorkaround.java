package com.amazon.connector.s3;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Closeable;
import lombok.SneakyThrows;
import org.junit.jupiter.api.function.ThrowingSupplier;

/**
 * This class is here to help work around certain SpotBugs quirks. Specifically, SpotBugs don't
 * allow to apply suppressions to Lambdas
 */
public final class SpotBugsLambdaWorkaround {
  /**
   * In situations where a method or constructor return a class that implements Closeable, Spotbugs
   * wants to see it closed. When testing bounds and expecting exceptions, this is unnecessary, and
   * trying to work around this in tests results in a lot of boilerplate. This method absorbs all
   * the boilerplate and acts as `assertThrows`.
   *
   * @param expectedType exception type expected
   * @param executable code that returns something Closeable, and expected to throw
   * @param <T> exception type
   * @param <C> return type
   */
  @SneakyThrows
  public static <T extends Throwable, C extends Closeable> void assertThrowsClosableResult(
      Class<T> expectedType, ThrowingSupplier<C> executable) {
    try (C result = executable.get()) {
    } catch (Throwable throwable) {
      assertInstanceOf(expectedType, throwable);
      return;
    }
    fail(String.format("Exception of type '%s' was expected. Nothing was thrown", expectedType));
  }
}
