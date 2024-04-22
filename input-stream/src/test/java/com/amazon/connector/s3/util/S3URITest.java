package com.amazon.connector.s3.util;

import static org.junit.Assert.assertThrows;

import org.junit.jupiter.api.Test;

public class S3URITest {

  @Test
  void testCreateValidatesArguments() {
    assertThrows(NullPointerException.class, () -> S3URI.of(null, "key"));
    assertThrows(NullPointerException.class, () -> S3URI.of("bucket", null));
  }
}
