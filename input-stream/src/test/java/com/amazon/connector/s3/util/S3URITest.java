package com.amazon.connector.s3.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
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

  @Test
  void testToString() {
    S3URI s3URI = S3URI.of("bucket", "key");
    assertEquals("s3://bucket/key", s3URI.toString());
    assertEquals("s3a://bucket/key", s3URI.toString("s3a"));
    assertThrows(NullPointerException.class, () -> s3URI.toString(null));
  }

  @Test
  void testToUri() {
    S3URI s3URI = S3URI.of("bucket", "key");
    assertEquals(URI.create("s3://bucket/key"), s3URI.toURI());
    assertEquals(URI.create("s3a://bucket/key"), s3URI.toURI("s3a"));
    assertThrows(NullPointerException.class, () -> s3URI.toURI(null));
  }
}
