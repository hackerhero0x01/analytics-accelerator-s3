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
package software.amazon.s3.dataaccelerator;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

/** Helper class for custom assertions. */
public final class CustomAssertions {

  /**
   * Asserts that a string contains other substrings and displays an easy to interpret message when
   * it does not.
   *
   * @param content the String to search in
   * @param substr the substrings to search for
   */
  public static void assertContains(String content, String... substr) {
    Arrays.stream(substr)
        .sequential()
        .forEach(
            str -> {
              assertTrue(
                  content.contains(str),
                  String.format("Expected '%s' to contain '%s'", content, str));
            });
  }
}
