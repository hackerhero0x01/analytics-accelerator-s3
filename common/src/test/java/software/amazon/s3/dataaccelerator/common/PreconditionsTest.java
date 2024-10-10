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
package software.amazon.s3.dataaccelerator.common;
/*
 * This code is based on https://github.com/google/guava/blob/master/guava-tests/test/com/google/common/base/PreconditionsTest.java
 * * This is to eliminate the dependency footprint on the full Guava.
 * Corresponding license and copyright are provided below.
 */

/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

import static org.junit.jupiter.api.Assertions.*;
import static software.amazon.s3.dataaccelerator.common.Preconditions.lenientFormat;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

public class PreconditionsTest {
  @Test
  public void testCheckArgument_simple_success() {
    Preconditions.checkArgument(true);
  }

  @Test
  public void testCheckArgument_simple_failure() {
    try {
      Preconditions.checkArgument(false);
      fail("no exception thrown");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testCheckArgument_simpleMessage_success() {
    Preconditions.checkArgument(true, IGNORE_ME);
  }

  @Test
  public void testCheckArgument_simpleMessage_failure() {
    try {
      Preconditions.checkArgument(false, new Message());
      fail("no exception thrown");
    } catch (IllegalArgumentException expected) {
      verifySimpleMessage(expected);
    }
  }

  @Test
  public void testCheckArgument_nullMessage_failure() {
    try {
      Preconditions.checkArgument(false, null);
      fail("no exception thrown");
    } catch (IllegalArgumentException expected) {
      assertEquals("null", expected.getMessage());
    }
  }

  @Test
  public void testCheckArgument_nullMessageWithArgs_failure() {
    try {
      Preconditions.checkArgument(false, null, "b", "d");
      fail("no exception thrown");
    } catch (IllegalArgumentException e) {
      assertEquals("null [b, d]", e.getMessage());
    }
  }

  @Test
  public void testCheckArgument_nullArgs_failure() {
    try {
      Preconditions.checkArgument(false, "A %s C %s E", null, null);
      fail("no exception thrown");
    } catch (IllegalArgumentException e) {
      assertEquals("A null C null E", e.getMessage());
    }
  }

  @Test
  public void testCheckArgument_notEnoughArgs_failure() {
    try {
      Preconditions.checkArgument(false, "A %s C %s E", "b");
      fail("no exception thrown");
    } catch (IllegalArgumentException e) {
      assertEquals("A b C %s E", e.getMessage());
    }
  }

  @Test
  public void testCheckArgument_tooManyArgs_failure() {
    try {
      Preconditions.checkArgument(false, "A %s C %s E", "b", "d", "f");
      fail("no exception thrown");
    } catch (IllegalArgumentException e) {
      assertEquals("A b C d E [f]", e.getMessage());
    }
  }

  @Test
  public void testCheckArgument_singleNullArg_failure() {
    try {
      Preconditions.checkArgument(false, "A %s C", (Object) null);
      fail("no exception thrown");
    } catch (IllegalArgumentException e) {
      assertEquals("A null C", e.getMessage());
    }
  }

  @Test
  public void testCheckArgument_singleNullArray_failure() {
    try {
      Preconditions.checkArgument(false, "A %s C", (Object[]) null);
      fail("no exception thrown");
    } catch (IllegalArgumentException e) {
      assertEquals("A (Object[])null C", e.getMessage());
    }
  }

  @Test
  public void testCheckArgument_formatMessage_success() {
    Preconditions.checkArgument(true, "%s", new Object[] {IGNORE_ME});
  }

  @Test
  public void testCheckArgument_formatMessage_failure() {
    try {
      Preconditions.checkArgument(false, FORMAT, new Object[] {5});
      fail("no exception thrown");
    } catch (IllegalArgumentException expected) {
      verifyComplexMessage(expected);
    }
  }

  @Test
  public void testCheckArgument_complexMessage_success() {
    Preconditions.checkArgument(true, "%s", IGNORE_ME);
  }

  @Test
  public void testCheckArgument_complexMessage_failure() {
    try {
      Preconditions.checkArgument(false, FORMAT, 5);
      fail("no exception thrown");
    } catch (IllegalArgumentException expected) {
      verifyComplexMessage(expected);
    }
  }

  @Test
  public void testCheckState_simple_success() {
    Preconditions.checkState(true);
  }

  @Test
  public void testCheckState_simple_failure() {
    try {
      Preconditions.checkState(false);
      fail("no exception thrown");
    } catch (IllegalStateException expected) {
    }
  }

  @Test
  public void testCheckState_simpleMessage_success() {
    Preconditions.checkState(true, IGNORE_ME);
  }

  @Test
  public void testCheckState_simpleMessage_failure() {
    try {
      Preconditions.checkState(false, new Message());
      fail("no exception thrown");
    } catch (IllegalStateException expected) {
      verifySimpleMessage(expected);
    }
  }

  @Test
  public void testCheckState_nullMessage_failure() {
    try {
      Preconditions.checkState(false, null);
      fail("no exception thrown");
    } catch (IllegalStateException expected) {
    } catch (IllegalArgumentException e) {
      assertEquals("null", e.getMessage());
    }
  }

  @Test
  public void testCheckState_complexMessage_success() {
    Preconditions.checkState(true, "%s", IGNORE_ME);
  }

  @Test
  public void testCheckState_complexMessage_failure() {
    try {
      Preconditions.checkState(false, FORMAT, 5);
      fail("no exception thrown");
    } catch (IllegalStateException expected) {
      verifyComplexMessage(expected);
    }
  }

  @Test
  public void testCheckState_formatString_success() {
    Preconditions.checkState(true, FORMAT, new Object[] {5});
  }

  @Test
  public void testCheckState_formatString_failure() {
    try {
      Preconditions.checkState(false, FORMAT, new Object[] {5});
      fail("no exception thrown");
    } catch (IllegalStateException expected) {
      verifyComplexMessage(expected);
    }
  }

  private static final String NON_NULL_STRING = "foo";

  @Test
  public void testCheckNotNull_simple_success() {
    String result = Preconditions.checkNotNull(NON_NULL_STRING);
    assertSame(NON_NULL_STRING, result);
  }

  @Test
  @SuppressFBWarnings(
      value = "DCN_NULLPOINTER_EXCEPTION",
      justification = "Catching NullPointerException for testing purposes")
  public void testCheckNotNull_simple_failure() {
    try {
      Preconditions.checkNotNull(null);
      fail("no exception thrown");
    } catch (NullPointerException expected) {
    }
  }

  @Test
  public void testCheckNotNull_simpleMessage_success() {
    String result = Preconditions.checkNotNull(NON_NULL_STRING, IGNORE_ME);
    assertSame(NON_NULL_STRING, result);
  }

  @Test
  @SuppressFBWarnings(
      value = "DCN_NULLPOINTER_EXCEPTION",
      justification = "Catching NullPointerException for testing purposes")
  public void testCheckNotNull_simpleMessage_failure() {
    try {
      Preconditions.checkNotNull(null, new Message());
      fail("no exception thrown");
    } catch (NullPointerException expected) {
      verifySimpleMessage(expected);
    }
  }

  @Test
  public void testCheckNotNull_complexMessage_success() {
    String result = Preconditions.checkNotNull(NON_NULL_STRING, "%s", IGNORE_ME);
    assertSame(NON_NULL_STRING, result);
  }

  @Test
  @SuppressFBWarnings(
      value = "DCN_NULLPOINTER_EXCEPTION",
      justification = "Catching NullPointerException for testing purposes")
  public void testCheckNotNull_complexMessage_failure() {
    try {
      Preconditions.checkNotNull(null, FORMAT, 5);
      fail("no exception thrown");
    } catch (NullPointerException expected) {
      verifyComplexMessage(expected);
    }
  }

  @Test
  public void testCheckNotNull_formatString_success() {
    Preconditions.checkNotNull(NON_NULL_STRING, FORMAT, new Object[] {5});
  }

  @Test
  @SuppressFBWarnings(
      value = "DCN_NULLPOINTER_EXCEPTION",
      justification = "Catching NullPointerException for testing purposes")
  public void testCheckNotNull_formatString_failure() {
    try {
      Preconditions.checkNotNull(null, FORMAT, new Object[] {5});
      fail("no exception thrown");
    } catch (NullPointerException expected) {
      verifyComplexMessage(expected);
    }
  }

  @Test
  public void testCheckElementIndex_ok() {
    assertEquals(0, Preconditions.checkElementIndex(0, 1));
    assertEquals(0, Preconditions.checkElementIndex(0, 2));
    assertEquals(1, Preconditions.checkElementIndex(1, 2));
  }

  @Test
  public void testCheckElementIndex_badSize() {
    try {
      Preconditions.checkElementIndex(1, -1);
      fail();
    } catch (IllegalArgumentException expected) {
      // don't care what the message text is, as this is an invalid usage of
      // the Preconditions class, unlike all the other exceptions it throws
    }
  }

  @Test
  public void testCheckElementIndex_negative() {
    try {
      Preconditions.checkElementIndex(-1, 1);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      assertEquals("index (-1) must not be negative", expected.getMessage());
    }
  }

  @Test
  public void testCheckElementIndex_tooHigh() {
    try {
      Preconditions.checkElementIndex(1, 1);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      assertEquals("index (1) must be less than size (1)", expected.getMessage());
    }
  }

  @Test
  public void testCheckElementIndex_withDesc_negative() {
    try {
      Preconditions.checkElementIndex(-1, 1, "foo");
      fail();
    } catch (IndexOutOfBoundsException expected) {
      assertEquals("foo (-1) must not be negative", expected.getMessage());
    }
  }

  @Test
  public void testCheckElementIndex_withDesc_tooHigh() {
    try {
      Preconditions.checkElementIndex(1, 1, "foo");
      fail();
    } catch (IndexOutOfBoundsException expected) {
      assertEquals("foo (1) must be less than size (1)", expected.getMessage());
    }
  }

  @Test
  public void testCheckPositionIndex_ok() {
    assertEquals(0, Preconditions.checkPositionIndex(0, 0));
    assertEquals(0, Preconditions.checkPositionIndex(0, 1));
    assertEquals(1, Preconditions.checkPositionIndex(1, 1));
  }

  @Test
  public void testCheckPositionIndex_badSize() {
    try {
      Preconditions.checkPositionIndex(1, -1);
      fail();
    } catch (IllegalArgumentException expected) {
      // don't care what the message text is, as this is an invalid usage of
      // the Preconditions class, unlike all the other exceptions it throws
    }
  }

  @Test
  public void testCheckPositionIndex_negative() {
    try {
      Preconditions.checkPositionIndex(-1, 1);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      assertEquals("index (-1) must not be negative", expected.getMessage());
    }
  }

  @Test
  public void testCheckPositionIndex_tooHigh() {
    try {
      Preconditions.checkPositionIndex(2, 1);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      assertEquals("index (2) must not be greater than size (1)", expected.getMessage());
    }
  }

  @Test
  public void testCheckPositionIndex_withDesc_negative() {
    try {
      Preconditions.checkPositionIndex(-1, 1, "foo");
      fail();
    } catch (IndexOutOfBoundsException expected) {
      assertEquals("foo (-1) must not be negative", expected.getMessage());
    }
  }

  @Test
  public void testCheckPositionIndex_withDesc_tooHigh() {
    try {
      Preconditions.checkPositionIndex(2, 1, "foo");
      fail();
    } catch (IndexOutOfBoundsException expected) {
      assertEquals("foo (2) must not be greater than size (1)", expected.getMessage());
    }
  }

  @Test
  public void testCheckPositionIndexes_ok() {
    Preconditions.checkPositionIndexes(0, 0, 0);
    Preconditions.checkPositionIndexes(0, 0, 1);
    Preconditions.checkPositionIndexes(0, 1, 1);
    Preconditions.checkPositionIndexes(1, 1, 1);
  }

  @Test
  public void testCheckPositionIndexes_badSize() {
    try {
      Preconditions.checkPositionIndexes(1, 1, -1);
      fail();
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testCheckPositionIndex_startNegative() {
    try {
      Preconditions.checkPositionIndexes(-1, 1, 1);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      assertEquals("start index (-1) must not be negative", expected.getMessage());
    }
  }

  @Test
  public void testCheckPositionIndex_endNegative() {
    try {
      Preconditions.checkPositionIndexes(0, -1, 1);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      assertEquals("end index (-1) must not be negative", expected.getMessage());
    }
  }

  @Test
  public void testCheckPositionIndexes_endTooHigh() {
    try {
      Preconditions.checkPositionIndexes(0, 2, 1);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      assertEquals("end index (2) must not be greater than size (1)", expected.getMessage());
    }
  }

  @Test
  public void testCheckPositionIndexes_reversed() {
    try {
      Preconditions.checkPositionIndexes(1, 0, 1);
      fail();
    } catch (IndexOutOfBoundsException expected) {
      assertEquals("end index (0) must not be less than start index (1)", expected.getMessage());
    }
  }

  @Test
  public void testAllOverloads_checkArgument() throws Exception {
    for (List<Class<?>> sig : allSignatures(boolean.class)) {
      Method checkArgumentMethod =
          Preconditions.class.getMethod("checkArgument", sig.toArray(new Class<?>[] {}));
      checkArgumentMethod.invoke(null /* static method */, getParametersForSignature(true, sig));

      Object[] failingParams = getParametersForSignature(false, sig);
      InvocationTargetException ite =
          assertThrows(
              InvocationTargetException.class,
              () -> checkArgumentMethod.invoke(null /* static method */, failingParams));
      assertFailureCause(ite.getCause(), IllegalArgumentException.class, failingParams);
    }
  }

  @Test
  public void testAllOverloads_checkState() throws Exception {
    for (List<Class<?>> sig : allSignatures(boolean.class)) {
      Method checkArgumentMethod =
          Preconditions.class.getMethod("checkState", sig.toArray(new Class<?>[] {}));
      checkArgumentMethod.invoke(null /* static method */, getParametersForSignature(true, sig));

      Object[] failingParams = getParametersForSignature(false, sig);
      InvocationTargetException ite =
          assertThrows(
              InvocationTargetException.class,
              () -> checkArgumentMethod.invoke(null /* static method */, failingParams));
      assertFailureCause(ite.getCause(), IllegalStateException.class, failingParams);
    }
  }

  @Test
  public void testAllOverloads_checkNotNull() throws Exception {
    for (List<Class<?>> sig : allSignatures(Object.class)) {
      Method checkArgumentMethod =
          Preconditions.class.getMethod("checkNotNull", sig.toArray(new Class<?>[] {}));
      checkArgumentMethod.invoke(
          null /* static method */, getParametersForSignature(new Object(), sig));

      Object[] failingParams = getParametersForSignature(null, sig);
      InvocationTargetException ite =
          assertThrows(
              InvocationTargetException.class,
              () -> checkArgumentMethod.invoke(null /* static method */, failingParams));
      assertFailureCause(ite.getCause(), NullPointerException.class, failingParams);
    }
  }

  /**
   * Asserts that the given throwable has the given class and then asserts on the message as using
   * the full set of method parameters.
   */
  private void assertFailureCause(
      Throwable throwable, Class<? extends Throwable> clazz, Object[] params) {
    assertInstanceOf(clazz, throwable);
    if (params.length == 1) {
      assertNull(throwable.getMessage());
    } else if (params.length == 2) {
      assertEquals("", throwable.getMessage());
    } else {
      assertEquals(
          lenientFormat("", Arrays.copyOfRange(params, 2, params.length)), throwable.getMessage());
    }
  }

  /**
   * Returns an array containing parameters for invoking a checkArgument, checkNotNull or checkState
   * method reflectively
   *
   * @param firstParam The first parameter
   * @param sig The method signature
   */
  private Object[] getParametersForSignature(Object firstParam, List<Class<?>> sig) {
    Object[] params = new Object[sig.size()];
    params[0] = firstParam;
    if (params.length > 1) {
      params[1] = "";
      if (params.length > 2) {
        // fill in the rest of the array with arbitrary instances
        for (int i = 2; i < params.length; i++) {
          params[i] = getArbitraryInstance(sig.get(i));
        }
      }
    }
    return params;
  }

  @SuppressWarnings("unchecked")
  private static <T> T getArbitraryInstance(Class<T> type) {
    if (type == char.class) {
      return (T) Character.class.cast(' ');
    } else if (type == String.class) {
      return type.cast("");
    } else if (type == int.class) {
      return (T) Integer.class.cast(0);
    } else if (type == long.class) {
      return (T) Long.class.cast(0L);
    } else if (type == Object.class) {
      return type.cast("");
    } else {
      throw new IllegalArgumentException("Unsupported type: " + type.getCanonicalName());
    }
  }

  private static final List<Class<?>> possibleParamTypes =
      Stream.of(char.class, int.class, long.class, Object.class).collect(Collectors.toList());

  /**
   * Returns a list of parameters for invoking an overload of checkState, checkArgument or
   * checkNotNull
   *
   * @param predicateType The first parameter to the method (boolean or Object)
   */
  private static List<List<Class<?>>> allSignatures(Class<?> predicateType) {
    List<List<Class<?>>> allOverloads = new ArrayList<>();
    // The first two are for the overloads that don't take formatting args, e.g.
    // checkArgument(boolean) and checkArgument(boolean, Object)
    allOverloads.add(Stream.of(predicateType).collect(Collectors.toList()));
    allOverloads.add(Stream.of(predicateType, Object.class).collect(Collectors.toList()));
    allOverloads.add(
        Stream.of(predicateType, String.class, Object.class, Object.class, Object.class)
            .collect(Collectors.toList()));
    allOverloads.add(
        Stream.of(
                predicateType, String.class, Object.class, Object.class, Object.class, Object.class)
            .collect(Collectors.toList()));
    for (Class<?> parameterTypeFirst : possibleParamTypes) {
      allOverloads.add(
          Stream.of(predicateType, String.class, parameterTypeFirst).collect(Collectors.toList()));
      for (Class<?> parameterTypeSecond : possibleParamTypes) {
        allOverloads.add(
            Stream.of(predicateType, String.class, parameterTypeFirst, parameterTypeSecond)
                .collect(Collectors.toList()));
      }
    }

    return allOverloads;
  }

  @Test
  public void testNullPointers() {
    /*
     * Don't bother testing: Preconditions defines a bunch of methods that accept a template (or
     * even entire message) that simultaneously:
     *
     * - _shouldn't_ be null, so we don't annotate it with @Nullable
     *
     * - _can_ be null without causing a runtime failure (because we don't want the interesting
     *   details of precondition failure to be hidden by an exception we throw about an unexpectedly
     *   null _failure message_)
     *
     * That combination upsets NullPointerTester, which wants any call that passes null for a
     * non-@Nullable parameter to trigger a NullPointerException.
     *
     * (We still define this empty method to keep PackageSanityTests from generating its own
     * automated nullness tests, which would fail.)
     */
  }

  private static final Object IGNORE_ME =
      new Object() {
        @Override
        public String toString() {
          throw new AssertionFailedError();
        }
      };

  private static class Message {
    boolean invoked;

    @Override
    public String toString() {
      assertFalse(invoked);
      invoked = true;
      return "A message";
    }
  }

  private static final String FORMAT = "I ate %s pies.";

  private static void verifySimpleMessage(Exception e) {
    assertEquals("A message", e.getMessage());
  }

  private static void verifyComplexMessage(Exception e) {
    assertEquals("I ate 5 pies.", e.getMessage());
  }
}
