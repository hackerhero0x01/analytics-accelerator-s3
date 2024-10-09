package com.amazon.connector.s3.access;

import static org.junit.jupiter.api.Assertions.assertEquals;

import software.amazon.awssdk.core.checksums.SdkChecksum;

public class ChecksumAssertions {
  /**
   * Checksum assertion
   *
   * @param expected expected checksum
   * @param actual actual checksum
   */
  public static void assertChecksums(SdkChecksum expected, SdkChecksum actual) {
    assertEquals(
        expected.getValue(),
        actual.getValue(),
        "Checksums of read data do not match! Possible correctness issue");
  }
}
