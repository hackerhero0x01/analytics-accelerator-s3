package com.amazon.connector.s3.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.jupiter.api.Test;

public class S3URITest {

  @Test
  void testCreateValidatesArguments() {
    assertThrows(NullPointerException.class, () -> S3URI.of(null, "key"));
    assertThrows(NullPointerException.class, () -> S3URI.of("bucket", null));
  }

  @Test
  void testCreate() {
    S3URI s3URI = S3URI.of("bucket", "key");

    assertEquals("bucket", s3URI.getBucket());
    assertEquals("key", s3URI.getKey());
  }
}
