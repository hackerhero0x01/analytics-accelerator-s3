package com.amazon.connector.s3.request;

import com.amazon.connector.s3.common.Preconditions;
import lombok.Getter;

/**
 * Object representing a byte range. This class helps us abstract away from S3 SDK constructs and
 * helps with testing. For example, it gets rid of the need of having to generate and parse strings
 * like "bytes=0-555" -- this is SDK detail we should not care about in layers above Object Client.
 */
public class Range {
  @Getter long start;
  @Getter long end;

  /**
   * Construct a range. At least one of the start and end of range should be present.
   *
   * @param start the start of the range, possibly empty
   * @param end the end of the range, possibly empty
   */
  public Range(long start, long end) {
    Preconditions.checkArgument(start >= 0, "`start` must not be negative");
    Preconditions.checkArgument(end >= 0, "`end` must not be negative");
    Preconditions.checkArgument(start <= end, "`start` must be less than equal to `end`");

    this.start = start;
    this.end = end;
  }

  public long getSize() {
    return this.end - this.start + 1;
  }

  @Override
  public String toString() {
    return "bytes=" + start + "-" + end;
  }
}
