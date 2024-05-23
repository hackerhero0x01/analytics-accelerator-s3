package com.amazon.connector.s3.util;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

public class ConstantsTest {

  @Test
  void testConstructor() {
    assertNotNull(new Constants());
  }
}
