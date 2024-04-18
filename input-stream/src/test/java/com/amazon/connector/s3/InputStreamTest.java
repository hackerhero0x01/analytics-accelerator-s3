package com.amazon.connector.s3;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

public class InputStreamTest {
  @Test
  void testConstructor() {
    ObjectClient objectClient = mock(ObjectClient.class);
    InputStream inputStream = new InputStream(objectClient);
    assertNotNull(inputStream);
  }

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new InputStream(null);
        });
  }
}
