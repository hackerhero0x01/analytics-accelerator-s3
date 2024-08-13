package com.amazon.connector.s3.io.physical.plan;

import lombok.Data;

/** Range of bytes to read from S3. */
@Data
// TODO: this needs to be merged with "Range" from the object client
public class Range {
  private static final String TO_STRING_FORMAT = "[%d-%d]";
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

  /**
   * Returns the textual representation of {@link Range}.
   *
   * @return the textual representation of {@link Range}.
   */
  @Override
  public String toString() {
    return String.format(TO_STRING_FORMAT, start, end);
  }
}
