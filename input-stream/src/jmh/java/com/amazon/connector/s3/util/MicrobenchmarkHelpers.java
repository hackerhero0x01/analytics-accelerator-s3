package com.amazon.connector.s3.util;

import java.io.IOException;
import java.io.InputStream;

/** Utility class for helper functions used in micro-benchmarks. */
public class MicrobenchmarkHelpers {

  /**
   * Fully consumes an input stream and returns the number of read bytes.
   *
   * @param inputStream the input stream to consume
   * @return the number of bytes read out of the stream
   */
  public static long consumeStream(InputStream inputStream) throws IOException {
    byte[] b = new byte[4096];

    long totalReadBytes = 0;
    int readBytes;
    do {
      readBytes = inputStream.read(b, 0, b.length);

      if (readBytes > 0) {
        totalReadBytes += readBytes;
      }
    } while (readBytes > 0);

    return totalReadBytes;
  }
}
