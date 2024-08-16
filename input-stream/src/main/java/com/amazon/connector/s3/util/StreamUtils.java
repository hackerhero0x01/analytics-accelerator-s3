package com.amazon.connector.s3.util;

import static com.amazon.connector.s3.util.Constants.ONE_KB;

import com.amazon.connector.s3.request.ObjectContent;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/** Utility class for stream operations. */
public class StreamUtils {

  private static final int BUFFER_SIZE = 8 * ONE_KB;

  /**
   * Convert an InputStream from the underlying object to a byte array.
   *
   * @param objectContent the part of the object
   * @return a byte array
   */
  public static byte[] toByteArray(ObjectContent objectContent) {
    InputStream inStream = objectContent.getStream();
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    byte[] buffer = new byte[BUFFER_SIZE];

    try {
      int numBytesRead;
      while ((numBytesRead = inStream.read(buffer, 0, buffer.length)) != -1) {
        outStream.write(buffer, 0, numBytesRead);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return outStream.toByteArray();
  }
}
