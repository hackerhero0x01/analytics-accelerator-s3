package com.amazon.connector.s3.util;

import com.amazon.connector.s3.object.ObjectContent;
import java.io.IOException;
import org.apache.commons.io.IOUtils;

/** Utility class for stream operations. */
public class StreamUtils {

  /**
   * Convert an InputStream from the underlying object to a byte array.
   *
   * @param objectContent the part of the object
   * @return a byte array
   */
  public static byte[] toByteArray(ObjectContent objectContent) {
    try {
      return IOUtils.toByteArray(objectContent.getStream());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
