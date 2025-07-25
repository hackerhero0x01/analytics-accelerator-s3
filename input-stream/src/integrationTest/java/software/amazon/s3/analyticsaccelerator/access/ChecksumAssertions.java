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
package software.amazon.s3.analyticsaccelerator.access;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ChecksumAssertions {
  /**
   * Checksum assertion
   *
   * @param expected expected checksum
   * @param actual actual checksum
   */
  public static void assertChecksums(Crc32CChecksum expected, Crc32CChecksum actual) {
    assertEquals(
        expected.getValue(),
        actual.getValue(),
        "Checksums of read data do not match! Possible correctness issue");
  }
}
