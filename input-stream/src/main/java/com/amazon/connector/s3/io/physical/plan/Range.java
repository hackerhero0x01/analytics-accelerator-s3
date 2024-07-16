package com.amazon.connector.s3.io.physical.plan;

import lombok.Data;

/** Range of bytes to read from S3. */
@Data
public class Range {
  private final long start;
  private final long end;

  /**
   * Gets length of range.
   *
   * @return length of range
   */
  public long getLength() {
    return end - start + 1;
  }
}
