package com.amazon.connector.s3.request;

import com.amazon.connector.s3.common.Preconditions;
import lombok.Getter;

/**
 * Object representing a byte range. This class helps us abstract away from S3 SDK constructs and
 * helps with testing. For example, it gets rid of the need of having to generate and parse strings
 * like "bytes=0-555" -- this is SDK detail we should not care about in layers above Object Client.
 */
@Getter
public class Range {
  long start;
  long end;

  private static final String TO_HTTP_STRING_FORMAT = "[%d-%d]";
  private static final String TO_STRING_FORMAT = "bytes=%d-%d";

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

  /**
   * Return the size of the range.
   *
   * @return the size of the range in bytes
   */
  public long getSize() {
    return this.end - this.start + 1;
  }

  @Override
  public String toString() {
    return String.format(TO_STRING_FORMAT, start, end);
  }

  /**
   * Returns the string representation of {@link Range} used in S3 requests, as defined by the Http
   * RFC.
   *
   * @return the HTTP RFC compatible representation of {@link Range}.
   */
  public String toHttpString() {
    return String.format(TO_HTTP_STRING_FORMAT, start, end);
  }
}
