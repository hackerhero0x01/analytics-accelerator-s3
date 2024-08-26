package com.amazon.connector.s3.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazon.connector.s3.request.ObjectContent;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.input.NullInputStream;
import org.junit.jupiter.api.Test;

public class StreamUtilsTest {

  @Test
  public void testToByteArrayWorksWithEmptyStream() {
    // Given: objectContent with an empty stream
    ObjectContent objectContent = ObjectContent.builder().stream(NullInputStream.INSTANCE).build();

    // When: toByteArray is called
    byte[] buf = StreamUtils.toByteArray(objectContent);

    // Then: returned byte array is empty
    String content = new String(buf, StandardCharsets.UTF_8);
    assertEquals(0, buf.length);
    assertEquals("", content);
  }

  @Test
  public void testToByteArrayConvertsCorrectly() {
    // Given: objectContent with "Hello World" in it
    InputStream inputStream =
        new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8));
    ObjectContent objectContent = ObjectContent.builder().stream(inputStream).build();

    // When: toByteArray is called
    byte[] buf = StreamUtils.toByteArray(objectContent);

    // Then: 'Hello World' is returned
    assertEquals("Hello World", new String(buf, StandardCharsets.UTF_8));
  }
}
